use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

use serde::{Deserialize, Deserializer};
use tracing::{info, warn};

pub const BUILTIN_COST_SOURCE: &str = "builtin_model_family_pricing";
pub const MIXED_COST_SOURCE: &str = "mixed_pricing_sources";
const PRICING_FILE_ENV: &str = "CLAUDITOR_PRICING_FILE";

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ModelPricing {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write_5m: f64,
    pub cache_write_1h: f64,
}

impl<'de> Deserialize<'de> for ModelPricing {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawModelPricing {
            input: f64,
            output: f64,
            cache_read: f64,
            #[serde(default, alias = "cache_create")]
            cache_write_5m: Option<f64>,
            #[serde(default)]
            cache_write_1h: Option<f64>,
        }

        let raw = RawModelPricing::deserialize(deserializer)?;
        Ok(Self {
            input: raw.input,
            output: raw.output,
            cache_read: raw.cache_read,
            cache_write_5m: raw.cache_write_5m.unwrap_or(raw.input * 1.25),
            cache_write_1h: raw.cache_write_1h.unwrap_or(raw.input * 2.0),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EstimatedCostBreakdown {
    pub total_cost_dollars: f64,
    pub cost_source: String,
    pub trusted_for_budget_enforcement: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedPricing {
    pub pricing: ModelPricing,
    pub cost_source: String,
    pub trusted_for_budget_enforcement: bool,
}

#[derive(Clone, Debug)]
pub struct PricingCatalog {
    trusted_for_budget_enforcement: bool,
    catalog_source: String,
    family: HashMap<String, ModelPricing>,
    model: HashMap<String, ModelPricing>,
    builtin_only: bool,
}

#[derive(Default, Deserialize)]
struct PricingFile {
    #[serde(default)]
    trusted_for_budget_enforcement: bool,
    #[serde(default)]
    source_label: Option<String>,
    #[serde(default)]
    family: HashMap<String, ModelPricing>,
    #[serde(default)]
    model: HashMap<String, ModelPricing>,
}

pub static PRICING_CATALOG: LazyLock<PricingCatalog> = LazyLock::new(PricingCatalog::load_from_env);

impl PricingCatalog {
    pub fn builtin() -> Self {
        Self {
            trusted_for_budget_enforcement: false,
            catalog_source: BUILTIN_COST_SOURCE.to_string(),
            family: HashMap::new(),
            model: HashMap::new(),
            builtin_only: true,
        }
    }

    pub fn load_from_env() -> Self {
        let Some(path) = std::env::var(PRICING_FILE_ENV).ok() else {
            info!(
                cost_source = BUILTIN_COST_SOURCE,
                trusted_for_budget_enforcement = false,
                "using built-in model-family pricing"
            );
            return Self::builtin();
        };

        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) => {
                warn!(
                    path = %path,
                    error = %err,
                    "failed to read pricing file, falling back to built-in pricing"
                );
                return Self::builtin();
            }
        };

        match Self::from_toml_str(&text, &path) {
            Ok(catalog) => {
                info!(
                    path = %path,
                    cost_source = %catalog.catalog_source,
                    trusted_for_budget_enforcement = catalog.trusted_for_budget_enforcement,
                    exact_models = catalog.model.len(),
                    families = catalog.family.len(),
                    "loaded pricing catalog from file"
                );
                catalog
            }
            Err(err) => {
                warn!(
                    path = %path,
                    error = %err,
                    "failed to parse pricing file, falling back to built-in pricing"
                );
                Self::builtin()
            }
        }
    }

    pub fn from_toml_str(text: &str, source_path: &str) -> Result<Self, String> {
        let parsed: PricingFile =
            toml::from_str(text).map_err(|err| format!("parse pricing toml: {err}"))?;

        let family = parsed
            .family
            .into_iter()
            .map(|(key, pricing)| (key.trim().to_ascii_lowercase(), pricing))
            .collect::<HashMap<_, _>>();
        let model = parsed
            .model
            .into_iter()
            .map(|(key, pricing)| (key.trim().to_string(), pricing))
            .collect::<HashMap<_, _>>();

        let source_label = parsed
            .source_label
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| {
                Path::new(source_path)
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(|stem| stem.to_string())
            })
            .unwrap_or_else(|| "custom".to_string());

        Ok(Self {
            trusted_for_budget_enforcement: parsed.trusted_for_budget_enforcement,
            catalog_source: format!("pricing_file:{source_label}"),
            family,
            model,
            builtin_only: false,
        })
    }

    pub fn active_catalog_source(&self) -> &str {
        &self.catalog_source
    }

    pub fn trusted_for_budget_enforcement(&self) -> bool {
        self.trusted_for_budget_enforcement
    }

    pub fn resolve(&self, model: &str) -> ResolvedPricing {
        if let Some(pricing) = self.model.get(model) {
            return ResolvedPricing {
                pricing: *pricing,
                cost_source: self.catalog_source.clone(),
                trusted_for_budget_enforcement: !self.builtin_only
                    && self.trusted_for_budget_enforcement,
            };
        }

        if let Some(family) = family_for_model(model) {
            if let Some(pricing) = self.family.get(family) {
                return ResolvedPricing {
                    pricing: *pricing,
                    cost_source: self.catalog_source.clone(),
                    trusted_for_budget_enforcement: !self.builtin_only
                        && self.trusted_for_budget_enforcement,
                };
            }
        }

        ResolvedPricing {
            pricing: builtin_pricing(model),
            cost_source: BUILTIN_COST_SOURCE.to_string(),
            trusted_for_budget_enforcement: false,
        }
    }
}

pub fn active_catalog_source() -> String {
    PRICING_CATALOG.active_catalog_source().to_string()
}

pub fn trusted_for_budget_enforcement() -> bool {
    PRICING_CATALOG.trusted_for_budget_enforcement()
}

pub fn summarize_cost_sources(sources: &std::collections::HashSet<String>) -> String {
    match sources.len() {
        0 => active_catalog_source(),
        1 => sources
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(active_catalog_source),
        _ => MIXED_COST_SOURCE.to_string(),
    }
}

pub fn resolve_pricing(model: &str) -> ResolvedPricing {
    PRICING_CATALOG.resolve(model)
}

pub fn token_cost(tokens: u64, price_per_mtok: f64) -> f64 {
    (tokens as f64) * price_per_mtok / 1_000_000.0
}

pub fn estimate_cost_dollars(
    model: &str,
    input: u64,
    output: u64,
    cache_read: u64,
    cache_create: u64,
) -> EstimatedCostBreakdown {
    estimate_cost_dollars_with_cache_buckets(model, input, output, cache_read, cache_create, 0)
}

pub fn estimate_cost_dollars_with_cache_buckets(
    model: &str,
    input: u64,
    output: u64,
    cache_read: u64,
    cache_create_5m: u64,
    cache_create_1h: u64,
) -> EstimatedCostBreakdown {
    let resolved = resolve_pricing(model);
    let total_cost_dollars = token_cost(input, resolved.pricing.input)
        + token_cost(output, resolved.pricing.output)
        + token_cost(cache_read, resolved.pricing.cache_read)
        + token_cost(cache_create_5m, resolved.pricing.cache_write_5m)
        + token_cost(cache_create_1h, resolved.pricing.cache_write_1h);

    EstimatedCostBreakdown {
        total_cost_dollars,
        cost_source: resolved.cost_source,
        trusted_for_budget_enforcement: resolved.trusted_for_budget_enforcement,
    }
}

pub fn estimate_cache_rebuild_waste_dollars(
    model: &str,
    cache_create: u64,
) -> EstimatedCostBreakdown {
    estimate_cache_rebuild_waste_dollars_with_cache_buckets(model, cache_create, 0)
}

pub fn estimate_cache_rebuild_waste_dollars_with_cache_buckets(
    model: &str,
    cache_create_5m: u64,
    cache_create_1h: u64,
) -> EstimatedCostBreakdown {
    let resolved = resolve_pricing(model);
    let rebuild_delta_5m = (resolved.pricing.cache_write_5m - resolved.pricing.cache_read).max(0.0);
    let rebuild_delta_1h = (resolved.pricing.cache_write_1h - resolved.pricing.cache_read).max(0.0);

    EstimatedCostBreakdown {
        total_cost_dollars: token_cost(cache_create_5m, rebuild_delta_5m)
            + token_cost(cache_create_1h, rebuild_delta_1h),
        cost_source: resolved.cost_source,
        trusted_for_budget_enforcement: resolved.trusted_for_budget_enforcement,
    }
}

fn family_for_model(model: &str) -> Option<&'static str> {
    let model = model.to_ascii_lowercase();
    if model.contains("opus") {
        Some("opus")
    } else if model.contains("haiku") {
        Some("haiku")
    } else if model.contains("sonnet") {
        Some("sonnet")
    } else {
        None
    }
}

fn builtin_pricing(model: &str) -> ModelPricing {
    let model = model.to_ascii_lowercase();
    if model.contains("opus-4-7")
        || model.contains("opus-4.7")
        || model.contains("opus-4-6")
        || model.contains("opus-4.6")
        || model.contains("opus-4-5")
        || model.contains("opus-4.5")
    {
        ModelPricing {
            input: 5.0,
            output: 25.0,
            cache_read: 0.50,
            cache_write_5m: 6.25,
            cache_write_1h: 10.0,
        }
    } else if model.contains("opus") {
        ModelPricing {
            input: 15.0,
            output: 75.0,
            cache_read: 1.50,
            cache_write_5m: 18.75,
            cache_write_1h: 30.0,
        }
    } else if model.contains("haiku-4-5") || model.contains("haiku-4.5") {
        ModelPricing {
            input: 1.0,
            output: 5.0,
            cache_read: 0.10,
            cache_write_5m: 1.25,
            cache_write_1h: 2.0,
        }
    } else if model.contains("haiku") {
        ModelPricing {
            input: 0.80,
            output: 4.0,
            cache_read: 0.08,
            cache_write_5m: 1.00,
            cache_write_1h: 1.60,
        }
    } else {
        ModelPricing {
            input: 3.0,
            output: 15.0,
            cache_read: 0.30,
            cache_write_5m: 3.75,
            cache_write_1h: 6.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        estimate_cache_rebuild_waste_dollars,
        estimate_cache_rebuild_waste_dollars_with_cache_buckets, estimate_cost_dollars,
        estimate_cost_dollars_with_cache_buckets, PricingCatalog, BUILTIN_COST_SOURCE,
    };

    #[test]
    fn pricing_catalog_resolves_exact_model_family_and_builtin_fallback() {
        let catalog = PricingCatalog::from_toml_str(
            r#"
trusted_for_budget_enforcement = true

[family.sonnet]
input = 2.10
output = 10.50
cache_read = 0.21
cache_write_5m = 2.63
cache_write_1h = 4.20

[model."claude-sonnet-4-5-20250929"]
input = 1.95
output = 9.75
cache_read = 0.20
cache_create = 2.45
"#,
            "/tmp/contract-2026q2.toml",
        )
        .expect("parse catalog");

        let exact = catalog.resolve("claude-sonnet-4-5-20250929");
        assert_eq!(exact.cost_source, "pricing_file:contract-2026q2");
        assert!(exact.trusted_for_budget_enforcement);
        assert_eq!(exact.pricing.input, 1.95);
        assert_eq!(exact.pricing.cache_write_5m, 2.45);
        assert_eq!(exact.pricing.cache_write_1h, 3.90);

        let family = catalog.resolve("claude-sonnet-4-6-20260101");
        assert_eq!(family.cost_source, "pricing_file:contract-2026q2");
        assert!(family.trusted_for_budget_enforcement);
        assert_eq!(family.pricing.output, 10.50);
        assert_eq!(family.pricing.cache_write_1h, 4.20);

        let builtin = catalog.resolve("claude-haiku-4-5-20250929");
        assert_eq!(builtin.cost_source, BUILTIN_COST_SOURCE);
        assert!(!builtin.trusted_for_budget_enforcement);
        assert_eq!(builtin.pricing.input, 1.00);
        assert_eq!(builtin.pricing.cache_write_5m, 1.25);
        assert_eq!(builtin.pricing.cache_write_1h, 2.00);
    }

    #[test]
    fn estimate_cost_helpers_share_the_same_catalog() {
        let builtin = estimate_cost_dollars("claude-sonnet-4-5", 1_000_000, 0, 0, 0);
        assert_eq!(builtin.cost_source, BUILTIN_COST_SOURCE);
        assert!((builtin.total_cost_dollars - 3.0).abs() < f64::EPSILON);

        let waste = estimate_cache_rebuild_waste_dollars("claude-sonnet-4-5", 1_000_000);
        assert_eq!(waste.cost_source, BUILTIN_COST_SOURCE);
        assert!((waste.total_cost_dollars - 3.45).abs() < 1e-9);
    }

    #[test]
    fn builtin_pricing_covers_current_families_and_cache_buckets() {
        let opus = PricingCatalog::builtin().resolve("claude-opus-4-7-20260410");
        assert_eq!(opus.pricing.input, 5.0);
        assert_eq!(opus.pricing.output, 25.0);
        assert_eq!(opus.pricing.cache_read, 0.50);
        assert_eq!(opus.pricing.cache_write_5m, 6.25);
        assert_eq!(opus.pricing.cache_write_1h, 10.0);

        let sonnet = PricingCatalog::builtin().resolve("claude-sonnet-4-6-20260217");
        assert_eq!(sonnet.pricing.input, 3.0);
        assert_eq!(sonnet.pricing.cache_write_5m, 3.75);
        assert_eq!(sonnet.pricing.cache_write_1h, 6.0);

        let haiku = PricingCatalog::builtin().resolve("claude-haiku-4-5-20251001");
        assert_eq!(haiku.pricing.input, 1.0);
        assert_eq!(haiku.pricing.output, 5.0);
        assert_eq!(haiku.pricing.cache_read, 0.10);
        assert_eq!(haiku.pricing.cache_write_5m, 1.25);
        assert_eq!(haiku.pricing.cache_write_1h, 2.0);
    }

    #[test]
    fn estimate_cost_uses_cache_read_5m_and_1h_write_prices() {
        let cost = estimate_cost_dollars_with_cache_buckets(
            "claude-sonnet-4-6",
            1_000_000,
            1_000_000,
            1_000_000,
            1_000_000,
            1_000_000,
        );

        assert_eq!(cost.cost_source, BUILTIN_COST_SOURCE);
        assert!((cost.total_cost_dollars - 28.05).abs() < 1e-9);

        let waste = estimate_cache_rebuild_waste_dollars_with_cache_buckets(
            "claude-sonnet-4-6",
            1_000_000,
            1_000_000,
        );
        assert!((waste.total_cost_dollars - 9.15).abs() < 1e-9);
    }
}
