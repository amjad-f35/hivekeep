//! Trace sampling policy, with `OTEL_TRACES_SAMPLER` /
//! `OTEL_TRACES_SAMPLER_ARG` applied over a component default.

use std::fmt;

use opentelemetry_sdk::trace::Sampler;

use crate::env::Env;

/// Selects the sampler.
pub const TRACES_SAMPLER_ENV: &str = "OTEL_TRACES_SAMPLER";
/// The ratio for the ratio-based samplers.
pub const TRACES_SAMPLER_ARG_ENV: &str = "OTEL_TRACES_SAMPLER_ARG";

/// The default root sampling ratio for the control-plane binaries: low
/// volume, and their traces are what lifecycle debugging needs, so the
/// default leans generous.
pub const CONTROL_PLANE_TRACE_RATIO: f64 = 0.1;

/// A sampler, described in a form that can be compared and logged.
#[derive(Debug, Clone, PartialEq)]
pub enum SamplerSpec {
    /// Samples every trace.
    AlwaysOn,
    /// Samples nothing.
    AlwaysOff,
    /// Samples a fraction of trace IDs.
    TraceIdRatio(f64),
    /// Follows the parent's decision, using the inner sampler for roots.
    ParentBased(Box<SamplerSpec>),
}

impl SamplerSpec {
    /// The SDK sampler this describes.
    pub fn to_sdk(&self) -> Sampler {
        match self {
            SamplerSpec::AlwaysOn => Sampler::AlwaysOn,
            SamplerSpec::AlwaysOff => Sampler::AlwaysOff,
            SamplerSpec::TraceIdRatio(r) => Sampler::TraceIdRatioBased(*r),
            SamplerSpec::ParentBased(root) => Sampler::ParentBased(Box::new(root.to_sdk())),
        }
    }
}

impl fmt::Display for SamplerSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SamplerSpec::AlwaysOn => f.write_str("AlwaysOnSampler"),
            SamplerSpec::AlwaysOff => f.write_str("AlwaysOffSampler"),
            SamplerSpec::TraceIdRatio(r) => write!(f, "TraceIDRatioBased{{{r}}}"),
            SamplerSpec::ParentBased(root) => write!(f, "ParentBased{{root:{root}}}"),
        }
    }
}

/// A resolved sampling policy. The root fraction is kept alongside the
/// sampler because the gate router mirrors it into its proxy's random
/// sampling percentage, which roots the data-plane traces.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TraceSampling {
    sampler: Option<SamplerSpec>,
    root_ratio: f64,
}

impl TraceSampling {
    /// The sampler, or `None` for the zero value.
    pub fn sampler(&self) -> Option<&SamplerSpec> {
        self.sampler.as_ref()
    }

    /// The percentage (0 to 100) of parentless requests sampled at the root.
    pub fn root_sampling_percent(&self) -> f64 {
        self.root_ratio * 100.0
    }

    fn description(&self) -> String {
        self.sampler
            .as_ref()
            .map_or_else(|| "<unset>".to_string(), ToString::to_string)
    }
}

/// `ParentBased(TraceIdRatio(ratio))`, the ratio clamped to `[0, 1]`.
pub fn parent_ratio_sampling(ratio: f64) -> TraceSampling {
    let ratio = ratio.clamp(0.0, 1.0);
    TraceSampling {
        sampler: Some(SamplerSpec::ParentBased(Box::new(
            SamplerSpec::TraceIdRatio(ratio),
        ))),
        root_ratio: ratio,
    }
}

/// `ParentBased(AlwaysOff)`.
pub fn parent_never_sampling() -> TraceSampling {
    TraceSampling {
        sampler: Some(SamplerSpec::ParentBased(Box::new(SamplerSpec::AlwaysOff))),
        root_ratio: 0.0,
    }
}

/// Applies `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` on top of the
/// component default. An invalid value keeps the default and is logged,
/// where the SDK would fall open to sampling everything; that includes a
/// ratio sampler without an argument, which the spec reads as 1.0.
pub fn resolve_trace_sampling(env: &dyn Env, def: TraceSampling) -> TraceSampling {
    let name = env.lookup(TRACES_SAMPLER_ENV);
    let arg = env.lookup(TRACES_SAMPLER_ARG_ENV);
    match resolve(name.as_deref(), arg.as_deref(), &def) {
        Ok(s) => s,
        Err(err) => {
            tracing::warn!(
                sampler = name.as_deref().unwrap_or(""),
                arg = arg.as_deref().unwrap_or(""),
                default = %def.description(),
                err = %err,
                "Invalid trace sampler environment, keeping the component default"
            );
            def
        }
    }
}

fn is_ratio_sampler(name: &str) -> bool {
    matches!(name, "traceidratio" | "parentbased_traceidratio")
}

/// The sampler names the SDK understands, minus the remote ones. Any error
/// means the default is kept.
pub(crate) fn resolve(
    name: Option<&str>,
    arg: Option<&str>,
    def: &TraceSampling,
) -> Result<TraceSampling, String> {
    let Some(name) = name else {
        return Ok(def.clone());
    };
    let name = name.trim().to_lowercase();
    // Unlike the SDK, set-but-empty reads as unset: templated manifests can
    // render empty variables.
    if name.is_empty() {
        return Ok(def.clone());
    }
    let mut ratio = 0.0;
    if is_ratio_sampler(&name) {
        let trimmed = arg.unwrap_or("").trim();
        if trimmed.is_empty() {
            return Err(format!(
                "{TRACES_SAMPLER_ENV} {name:?} requires {TRACES_SAMPLER_ARG_ENV} to be a ratio in [0, 1]"
            ));
        }
        let parsed: f64 = trimmed.parse().map_err(|e| {
            format!(
                "parse {TRACES_SAMPLER_ARG_ENV} {:?}: {e}",
                arg.unwrap_or("")
            )
        })?;
        if !(0.0..=1.0).contains(&parsed) {
            return Err(format!(
                "{TRACES_SAMPLER_ARG_ENV} {:?} outside [0, 1]",
                arg.unwrap_or("")
            ));
        }
        ratio = parsed;
    }
    let on = || Box::new(SamplerSpec::AlwaysOn);
    Ok(match name.as_str() {
        "always_on" => TraceSampling {
            sampler: Some(SamplerSpec::AlwaysOn),
            root_ratio: 1.0,
        },
        "always_off" => TraceSampling {
            sampler: Some(SamplerSpec::AlwaysOff),
            root_ratio: 0.0,
        },
        "traceidratio" => TraceSampling {
            sampler: Some(SamplerSpec::TraceIdRatio(ratio)),
            root_ratio: ratio,
        },
        "parentbased_always_on" => TraceSampling {
            sampler: Some(SamplerSpec::ParentBased(on())),
            root_ratio: 1.0,
        },
        "parentbased_always_off" => parent_never_sampling(),
        "parentbased_traceidratio" => parent_ratio_sampling(ratio),
        _ => return Err(format!("unsupported {TRACES_SAMPLER_ENV} {name:?}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::MapEnv;

    const DEFAULT_RATIO: f64 = 0.25;

    fn pb(inner: SamplerSpec) -> SamplerSpec {
        SamplerSpec::ParentBased(Box::new(inner))
    }

    #[test]
    fn resolve_trace_sampling_table() {
        let def = parent_ratio_sampling(DEFAULT_RATIO);
        let def_s = def.sampler().unwrap().clone();
        let d = DEFAULT_RATIO * 100.0;
        #[allow(clippy::type_complexity)]
        let cases: Vec<(
            &str,
            Option<&str>,
            Option<&str>,
            TraceSampling,
            SamplerSpec,
            f64,
            bool,
        )> = vec![
            (
                "unset keeps ratio default",
                None,
                None,
                def.clone(),
                def_s.clone(),
                d,
                false,
            ),
            (
                "unset keeps never default",
                None,
                None,
                parent_never_sampling(),
                pb(SamplerSpec::AlwaysOff),
                0.0,
                false,
            ),
            (
                "set but empty keeps default",
                Some(""),
                None,
                def.clone(),
                def_s.clone(),
                d,
                false,
            ),
            (
                "always_on",
                Some("always_on"),
                None,
                def.clone(),
                SamplerSpec::AlwaysOn,
                100.0,
                false,
            ),
            (
                "always_off",
                Some("always_off"),
                None,
                def.clone(),
                SamplerSpec::AlwaysOff,
                0.0,
                false,
            ),
            (
                "traceidratio with arg",
                Some("traceidratio"),
                Some("0.5"),
                def.clone(),
                SamplerSpec::TraceIdRatio(0.5),
                50.0,
                false,
            ),
            (
                "parentbased_always_on",
                Some("parentbased_always_on"),
                None,
                def.clone(),
                pb(SamplerSpec::AlwaysOn),
                100.0,
                false,
            ),
            (
                "parentbased_always_off",
                Some("parentbased_always_off"),
                None,
                def.clone(),
                pb(SamplerSpec::AlwaysOff),
                0.0,
                false,
            ),
            (
                "parentbased_traceidratio with arg",
                Some("parentbased_traceidratio"),
                Some("0.5"),
                def.clone(),
                pb(SamplerSpec::TraceIdRatio(0.5)),
                50.0,
                false,
            ),
            (
                "mixed case and whitespace normalize",
                Some("  ParentBased_TraceIDRatio  "),
                Some(" 0.5 "),
                def.clone(),
                pb(SamplerSpec::TraceIdRatio(0.5)),
                50.0,
                false,
            ),
            (
                "ratio without arg keeps default",
                Some("parentbased_traceidratio"),
                None,
                def.clone(),
                def_s.clone(),
                d,
                true,
            ),
            (
                "ratio with empty arg keeps default",
                Some("traceidratio"),
                Some("  "),
                def.clone(),
                def_s.clone(),
                d,
                true,
            ),
            (
                "unknown sampler keeps default",
                Some("jaeger_remote"),
                None,
                def.clone(),
                def_s.clone(),
                d,
                true,
            ),
            (
                "unparsable arg keeps default",
                Some("parentbased_traceidratio"),
                Some("abc"),
                def.clone(),
                def_s.clone(),
                d,
                true,
            ),
            (
                "arg above 1 keeps default",
                Some("parentbased_traceidratio"),
                Some("1.5"),
                def.clone(),
                def_s.clone(),
                d,
                true,
            ),
            (
                "negative arg keeps default",
                Some("traceidratio"),
                Some("-0.1"),
                def.clone(),
                def_s.clone(),
                d,
                true,
            ),
        ];
        for (name, sampler, arg, def, want, want_pct, want_err) in cases {
            let res = resolve(sampler, arg, &def);
            assert_eq!(res.is_err(), want_err, "{name}: {res:?}");
            let got = res.unwrap_or(def);
            assert_eq!(got.sampler(), Some(&want), "{name}");
            assert_eq!(got.root_sampling_percent(), want_pct, "{name}");
        }
    }

    #[test]
    fn resolve_trace_sampling_reads_env() {
        let def = parent_never_sampling();
        let env = MapEnv::new([
            (TRACES_SAMPLER_ENV, "parentbased_traceidratio"),
            (TRACES_SAMPLER_ARG_ENV, "0.5"),
        ]);
        let got = resolve_trace_sampling(&env, def.clone());
        assert_eq!(got.sampler(), Some(&pb(SamplerSpec::TraceIdRatio(0.5))));
        let env = MapEnv::new([(TRACES_SAMPLER_ENV, "not_a_sampler")]);
        assert_eq!(resolve_trace_sampling(&env, def.clone()), def);
    }

    #[test]
    fn resolve_trace_sampling_zero_value_default() {
        let env = MapEnv::new([(TRACES_SAMPLER_ENV, "not_a_sampler")]);
        let got = resolve_trace_sampling(&env, TraceSampling::default());
        assert!(got.sampler().is_none());
        assert_eq!(got.root_sampling_percent(), 0.0);
    }

    #[test]
    fn parent_sampling_constructors() {
        assert_eq!(parent_ratio_sampling(0.01).root_sampling_percent(), 1.0);
        assert_eq!(parent_never_sampling().root_sampling_percent(), 0.0);
        assert_eq!(parent_ratio_sampling(1.5).root_sampling_percent(), 100.0);
        assert_eq!(parent_ratio_sampling(-1.0).root_sampling_percent(), 0.0);
    }

    #[test]
    fn sampler_descriptions() {
        assert_eq!(
            pb(SamplerSpec::TraceIdRatio(0.5)).to_string(),
            "ParentBased{root:TraceIDRatioBased{0.5}}"
        );
        assert!(matches!(SamplerSpec::AlwaysOn.to_sdk(), Sampler::AlwaysOn));
    }
}
