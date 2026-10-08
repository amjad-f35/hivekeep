//! Validators for the workload and sandbox-runtime messages that the public
//! API and the keeper API both define with identical fields and rules
//! (containers, volumes sources, resources, runtimes, ...).
//!
//! [`workload_validators!`] expands into one function per message, written
//! against a module alias `pb` that must be in scope where it is invoked, so
//! the rules are stated once and applied to both message families.

/// Expands the shared validators against the proto module aliased as `pb`.
macro_rules! workload_validators {
    () => {
        use std::collections::HashSet;

        use $crate::field::{ErrorList, FieldError, Path};
        use $crate::validation::checks;
        use $crate::validation::rules::{self, int, list, message, string, Format, Operation};

        /// An amount of compute resources.
        pub fn resources(
            op: &Operation,
            p: &Path,
            n: &pb::Resources,
            o: Option<&pb::Resources>,
        ) -> ErrorList {
            rules::all([
                int(
                    op,
                    &p.child("cpu_millicores"),
                    n.cpu_millicores,
                    o.map(|o| o.cpu_millicores),
                )
                .optional()
                .min(0)
                .max(999_999)
                .finish(),
                int(
                    op,
                    &p.child("memory_bytes"),
                    n.memory_bytes,
                    o.map(|o| o.memory_bytes),
                )
                .optional()
                .min(0)
                .finish(),
            ])
        }

        /// Resource limits.
        pub fn resource_requirements(
            op: &Operation,
            p: &Path,
            n: &pb::ResourceRequirements,
            o: Option<&pb::ResourceRequirements>,
        ) -> ErrorList {
            message(
                op,
                &p.child("limits"),
                n.limits.as_ref(),
                o.map(|o| o.limits.as_ref()),
            )
            .optional()
            .then(|p, n, o| resources(op, p, n, o))
            .finish()
        }

        /// One environment variable. The value is never echoed in errors.
        pub fn env_var(
            op: &Operation,
            p: &Path,
            n: &pb::EnvVar,
            o: Option<&pb::EnvVar>,
        ) -> ErrorList {
            rules::all([
                string(op, &p.child("name"), &n.name, o.map(|o| o.name.as_str()))
                    .required()
                    .max_len(256)
                    .custom(checks::env_var_name)
                    .finish(),
                string(op, &p.child("value"), &n.value, o.map(|o| o.value.as_str()))
                    .optional()
                    .max_len(32_768)
                    .finish(),
            ])
        }

        /// The HTTP GET of a wakeup probe.
        pub fn http_get_action(
            op: &Operation,
            p: &Path,
            n: &pb::HttpGetAction,
            o: Option<&pb::HttpGetAction>,
        ) -> ErrorList {
            rules::all([
                string(op, &p.child("path"), &n.path, o.map(|o| o.path.as_str()))
                    .required()
                    .max_len(1024)
                    .custom(checks::http_get_path)
                    .finish(),
                int(op, &p.child("port"), n.port, o.map(|o| o.port))
                    .required()
                    .min(1)
                    .max(65_535)
                    .finish(),
            ])
        }

        /// A wakeup probe.
        pub fn wakeup_probe(
            op: &Operation,
            p: &Path,
            n: &pb::WakeupProbe,
            o: Option<&pb::WakeupProbe>,
        ) -> ErrorList {
            rules::all([
                message(
                    op,
                    &p.child("http_get"),
                    n.http_get.as_ref(),
                    o.map(|o| o.http_get.as_ref()),
                )
                .required()
                .then(|p, n, o| http_get_action(op, p, n, o))
                .finish(),
                int(
                    op,
                    &p.child("timeout_seconds"),
                    n.timeout_seconds,
                    o.map(|o| o.timeout_seconds),
                )
                .required()
                .min(1)
                .max(3600)
                .finish(),
            ])
        }

        /// A volume mount.
        pub fn volume_mount(
            op: &Operation,
            p: &Path,
            n: &pb::VolumeMount,
            o: Option<&pb::VolumeMount>,
        ) -> ErrorList {
            rules::all([
                string(op, &p.child("name"), &n.name, o.map(|o| o.name.as_str()))
                    .required()
                    .format(Format::DnsLabel)
                    .finish(),
                string(
                    op,
                    &p.child("mount_path"),
                    &n.mount_path,
                    o.map(|o| o.mount_path.as_str()),
                )
                .required()
                .max_len(4096)
                .custom(checks::mount_path)
                .finish(),
            ])
        }

        /// Capability adjustments.
        pub fn capabilities(
            op: &Operation,
            p: &Path,
            n: &pb::Capabilities,
            o: Option<&pb::Capabilities>,
        ) -> ErrorList {
            rules::all([
                list(op, &p.child("add"), &n.add, o.map(|o| o.add.as_slice()))
                    .max_items(64)
                    .optional()
                    .unique_values()
                    .custom(|p, v| checks::capabilities(p, v, false))
                    .finish(),
                list(op, &p.child("drop"), &n.drop, o.map(|o| o.drop.as_slice()))
                    .max_items(64)
                    .optional()
                    .unique_values()
                    .custom(|p, v| checks::capabilities(p, v, true))
                    .finish(),
            ])
        }

        /// Process security settings.
        pub fn security_context(
            op: &Operation,
            p: &Path,
            n: &pb::SecurityContext,
            o: Option<&pb::SecurityContext>,
        ) -> ErrorList {
            message(
                op,
                &p.child("capabilities"),
                n.capabilities.as_ref(),
                o.map(|o| o.capabilities.as_ref()),
            )
            .optional()
            .then(|p, n, o| capabilities(op, p, n, o))
            .finish()
        }

        fn argv(op: &Operation, p: &Path, n: &[String], o: Option<&[String]>) -> ErrorList {
            list(op, p, n, o)
                .max_items(64)
                .optional()
                .each(|p, s, _| string(op, p, s, None).max_len(4096).finish())
                .finish()
        }

        /// One container.
        pub fn container(
            op: &Operation,
            p: &Path,
            n: &pb::Container,
            o: Option<&pb::Container>,
        ) -> ErrorList {
            rules::all([
                string(op, &p.child("name"), &n.name, o.map(|o| o.name.as_str()))
                    .required()
                    .format(Format::DnsLabel)
                    .finish(),
                string(op, &p.child("image"), &n.image, o.map(|o| o.image.as_str()))
                    .required()
                    .max_len(512)
                    .custom(checks::pinned_image)
                    .finish(),
                argv(
                    op,
                    &p.child("command"),
                    &n.command,
                    o.map(|o| o.command.as_slice()),
                ),
                argv(op, &p.child("args"), &n.args, o.map(|o| o.args.as_slice())),
                list(op, &p.child("env"), &n.env, o.map(|o| o.env.as_slice()))
                    .max_items(32)
                    .optional()
                    .each_keyed(|e| e.name.clone(), |p, n, o| env_var(op, p, n, o))
                    .unique_by(|e| e.name.clone())
                    .finish(),
                message(
                    op,
                    &p.child("wakeup_probe"),
                    n.wakeup_probe.as_ref(),
                    o.map(|o| o.wakeup_probe.as_ref()),
                )
                .optional()
                .then(|p, n, o| wakeup_probe(op, p, n, o))
                .finish(),
                list(
                    op,
                    &p.child("volume_mounts"),
                    &n.volume_mounts,
                    o.map(|o| o.volume_mounts.as_slice()),
                )
                .max_items(32)
                .optional()
                .each_keyed(
                    |m| m.mount_path.clone(),
                    |p, n, o| volume_mount(op, p, n, o),
                )
                .unique_by(|m| m.mount_path.clone())
                .custom(|p, v| {
                    let paths: Vec<&str> = v.iter().map(|m| m.mount_path.as_str()).collect();
                    checks::nested_mount_paths(p, &paths)
                })
                .finish(),
                message(
                    op,
                    &p.child("security_context"),
                    n.security_context.as_ref(),
                    o.map(|o| o.security_context.as_ref()),
                )
                .optional()
                .then(|p, n, o| security_context(op, p, n, o))
                .finish(),
                message(
                    op,
                    &p.child("resources"),
                    n.resources.as_ref(),
                    o.map(|o| o.resources.as_ref()),
                )
                .optional()
                .then(|p, n, o| resource_requirements(op, p, n, o))
                .finish(),
            ])
        }

        /// Reports container volume mounts that name no declared volume.
        pub fn mounts_reference_volumes(
            p: &Path,
            containers: &[pb::Container],
            volumes: &[pb::Volume],
            what: &str,
        ) -> ErrorList {
            let declared: HashSet<&str> = volumes.iter().map(|v| v.name.as_str()).collect();
            let mut errs = ErrorList::new();
            for (i, c) in containers.iter().enumerate() {
                for (j, m) in c.volume_mounts.iter().enumerate() {
                    if !m.name.is_empty() && !declared.contains(m.name.as_str()) {
                        errs.push(FieldError::invalid(
                            &p.child("containers")
                                .index(i)
                                .child("volume_mounts")
                                .index(j)
                                .child("name"),
                            &m.name,
                            format!("must reference a volume declared in the {what}"),
                        ));
                    }
                }
            }
            errs
        }

        /// A digest-pinned image volume.
        pub fn image_volume_source(
            op: &Operation,
            p: &Path,
            n: &pb::ImageVolumeSource,
            o: Option<&pb::ImageVolumeSource>,
        ) -> ErrorList {
            string(
                op,
                &p.child("reference"),
                &n.reference,
                o.map(|o| o.reference.as_str()),
            )
            .required()
            .max_len(512)
            .custom(checks::pinned_image)
            .finish()
        }

        fn projected(op: &Operation, p: &Path, n: &str, o: Option<&str>) -> ErrorList {
            string(op, p, n, o)
                .required()
                .min_len(1)
                .max_len(255)
                .custom(checks::projected_path)
                .finish()
        }

        /// One projected identity field.
        pub fn actor_metadata_item(
            op: &Operation,
            p: &Path,
            n: &pb::ActorMetadataItem,
            o: Option<&pb::ActorMetadataItem>,
        ) -> ErrorList {
            rules::all([
                int(op, &p.child("field"), n.field, o.map(|o| o.field))
                    .required()
                    .min(1)
                    .max(3)
                    .finish(),
                projected(op, &p.child("path"), &n.path, o.map(|o| o.path.as_str())),
            ])
        }

        /// Projected identity fields.
        pub fn actor_metadata_data_source(
            op: &Operation,
            p: &Path,
            n: &pb::ActorMetadataDataSource,
            o: Option<&pb::ActorMetadataDataSource>,
        ) -> ErrorList {
            list(
                op,
                &p.child("items"),
                &n.items,
                o.map(|o| o.items.as_slice()),
            )
            .max_items(8)
            .required()
            .min_items(1)
            .each(|p, n, o| actor_metadata_item(op, p, n, o))
            .unique_by(|i| i.field)
            .finish()
        }

        /// Projected trust bundles.
        pub fn trust_bundle_data_source(
            op: &Operation,
            p: &Path,
            n: &pb::TrustBundleDataSource,
            o: Option<&pb::TrustBundleDataSource>,
        ) -> ErrorList {
            rules::all([
                list(
                    op,
                    &p.child("names"),
                    &n.names,
                    o.map(|o| o.names.as_slice()),
                )
                .max_items(8)
                .required()
                .min_items(1)
                .each(|p, s, _| string(op, p, s, None).min_len(1).max_len(253).finish())
                .unique_values()
                .finish(),
                projected(op, &p.child("path"), &n.path, o.map(|o| o.path.as_str())),
            ])
        }

        /// One system-info projection; exactly one member is set.
        pub fn system_info_data_source(
            op: &Operation,
            p: &Path,
            n: &pb::SystemInfoDataSource,
            o: Option<&pb::SystemInfoDataSource>,
        ) -> ErrorList {
            rules::all([
                rules::union(
                    op,
                    p,
                    &[
                        ("actor_metadata", n.actor_metadata.is_some()),
                        ("trust_bundle", n.trust_bundle.is_some()),
                    ],
                    o.map(|o| [o.actor_metadata.is_some(), o.trust_bundle.is_some()])
                        .as_ref()
                        .map(|a| a.as_slice()),
                ),
                message(
                    op,
                    &p.child("actor_metadata"),
                    n.actor_metadata.as_ref(),
                    o.map(|o| o.actor_metadata.as_ref()),
                )
                .optional()
                .then(|p, n, o| actor_metadata_data_source(op, p, n, o))
                .finish(),
                message(
                    op,
                    &p.child("trust_bundle"),
                    n.trust_bundle.as_ref(),
                    o.map(|o| o.trust_bundle.as_ref()),
                )
                .optional()
                .then(|p, n, o| trust_bundle_data_source(op, p, n, o))
                .finish(),
            ])
        }

        /// At most one actor_metadata entry, and projected paths unique across
        /// every entry: they are written into one tree, so a repeat would
        /// silently overwrite an earlier file.
        fn data_sources_disjoint(p: &Path, sources: &[pb::SystemInfoDataSource]) -> ErrorList {
            let mut errs = ErrorList::new();
            let mut seen = HashSet::new();
            let mut saw_metadata = false;
            for (i, ds) in sources.iter().enumerate() {
                if let Some(tb) = &ds.trust_bundle {
                    if !seen.insert(tb.path.as_str()) {
                        errs.push(FieldError::duplicate(
                            &p.index(i).child("trust_bundle").child("path"),
                            &tb.path,
                        ));
                    }
                } else if let Some(md) = &ds.actor_metadata {
                    if saw_metadata {
                        errs.push(FieldError::forbidden(
                            &p.index(i).child("actor_metadata"),
                            "at most one actor_metadata entry may appear",
                        ));
                    }
                    saw_metadata = true;
                    for (j, item) in md.items.iter().enumerate() {
                        if !seen.insert(item.path.as_str()) {
                            errs.push(FieldError::duplicate(
                                &p.index(i)
                                    .child("actor_metadata")
                                    .child("items")
                                    .index(j)
                                    .child("path"),
                                &item.path,
                            ));
                        }
                    }
                }
            }
            errs
        }

        /// A system-info volume.
        pub fn system_info_volume_source(
            op: &Operation,
            p: &Path,
            n: &pb::SystemInfoVolumeSource,
            o: Option<&pb::SystemInfoVolumeSource>,
        ) -> ErrorList {
            list(
                op,
                &p.child("data_sources"),
                &n.data_sources,
                o.map(|o| o.data_sources.as_slice()),
            )
            .max_items(8)
            .optional()
            .each(|p, n, o| system_info_data_source(op, p, n, o))
            .custom(data_sources_disjoint)
            .finish()
        }

        /// One compatibility attribute.
        pub fn attribute_entry(
            op: &Operation,
            p: &Path,
            n: &pb::AttributeEntry,
            o: Option<&pb::AttributeEntry>,
        ) -> ErrorList {
            rules::all([
                string(op, &p.child("key"), &n.key, o.map(|o| o.key.as_str()))
                    .required()
                    .max_len(128)
                    .finish(),
                string(op, &p.child("value"), &n.value, o.map(|o| o.value.as_str()))
                    .required()
                    .max_len(256)
                    .finish(),
            ])
        }

        /// A runtime compatibility version.
        pub fn versioned_sandbox_compat(
            op: &Operation,
            p: &Path,
            n: &pb::VersionedSandboxCompat,
            o: Option<&pb::VersionedSandboxCompat>,
        ) -> ErrorList {
            rules::all([
                string(
                    op,
                    &p.child("schema_version"),
                    &n.schema_version,
                    o.map(|o| o.schema_version.as_str()),
                )
                .required()
                .max_len(64)
                .finish(),
                list(
                    op,
                    &p.child("attributes"),
                    &n.attributes,
                    o.map(|o| o.attributes.as_slice()),
                )
                .max_items(32)
                .required()
                .each_keyed(|a| a.key.clone(), |p, n, o| attribute_entry(op, p, n, o))
                .unique_by(|a| a.key.clone())
                .finish(),
            ])
        }

        /// The runtime an Actor runs under.
        pub fn actor_sandbox_runtime(
            op: &Operation,
            p: &Path,
            n: &pb::ActorSandboxRuntime,
            o: Option<&pb::ActorSandboxRuntime>,
        ) -> ErrorList {
            rules::all([
                string(
                    op,
                    &p.child("sandbox_class"),
                    &n.sandbox_class,
                    o.map(|o| o.sandbox_class.as_str()),
                )
                .required()
                .max_len(63)
                .custom(checks::sandbox_class)
                .finish(),
                message(
                    op,
                    &p.child("version"),
                    n.version.as_ref(),
                    o.map(|o| o.version.as_ref()),
                )
                .required()
                .then(|p, n, o| versioned_sandbox_compat(op, p, n, o))
                .finish(),
            ])
        }

        /// Per-Actor memory overhead of a runtime.
        pub fn runtime_overhead(
            op: &Operation,
            p: &Path,
            n: &pb::RuntimeOverhead,
            o: Option<&pb::RuntimeOverhead>,
        ) -> ErrorList {
            rules::all([
                int(
                    op,
                    &p.child("memory_bytes"),
                    n.memory_bytes,
                    o.map(|o| o.memory_bytes),
                )
                .optional()
                .min(0)
                .max(1 << 40)
                .finish(),
                int(
                    op,
                    &p.child("memory_bytes_per_gib"),
                    n.memory_bytes_per_gib,
                    o.map(|o| o.memory_bytes_per_gib),
                )
                .optional()
                .min(0)
                .max(1 << 30)
                .finish(),
            ])
        }

        /// One runtime a worker offers.
        pub fn worker_sandbox_runtime(
            op: &Operation,
            p: &Path,
            n: &pb::WorkerSandboxRuntime,
            o: Option<&pb::WorkerSandboxRuntime>,
        ) -> ErrorList {
            rules::all([
                message(
                    op,
                    &p.child("version"),
                    n.version.as_ref(),
                    o.map(|o| o.version.as_ref()),
                )
                .required()
                .then(|p, n, o| versioned_sandbox_compat(op, p, n, o))
                .finish(),
                message(
                    op,
                    &p.child("actor_overhead"),
                    n.actor_overhead.as_ref(),
                    o.map(|o| o.actor_overhead.as_ref()),
                )
                .optional()
                .then(|p, n, o| runtime_overhead(op, p, n, o))
                .finish(),
            ])
        }

        /// One sandbox class a worker runs.
        pub fn worker_sandbox_class(
            op: &Operation,
            p: &Path,
            n: &pb::WorkerSandboxClass,
            o: Option<&pb::WorkerSandboxClass>,
        ) -> ErrorList {
            rules::all([
                string(
                    op,
                    &p.child("sandbox_class"),
                    &n.sandbox_class,
                    o.map(|o| o.sandbox_class.as_str()),
                )
                .required()
                .max_len(63)
                .custom(checks::sandbox_class)
                .finish(),
                message(
                    op,
                    &p.child("default_runtime"),
                    n.default_runtime.as_ref(),
                    o.map(|o| o.default_runtime.as_ref()),
                )
                .required()
                .then(|p, n, o| worker_sandbox_runtime(op, p, n, o))
                .finish(),
                list(
                    op,
                    &p.child("restorable_runtimes"),
                    &n.restorable_runtimes,
                    o.map(|o| o.restorable_runtimes.as_slice()),
                )
                .max_items(32)
                .optional()
                .each(|p, n, o| worker_sandbox_runtime(op, p, n, o))
                .finish(),
            ])
        }

        /// The sandbox classes of a worker (at most one today).
        pub fn worker_sandbox_classes(
            op: &Operation,
            p: &Path,
            n: &[pb::WorkerSandboxClass],
            o: Option<&[pb::WorkerSandboxClass]>,
            required: bool,
        ) -> ErrorList {
            let l = list(op, p, n, o).max_items(1);
            let l = if required {
                l.required().min_items(1)
            } else {
                l.optional()
            };
            l.each_keyed(
                |c| c.sandbox_class.clone(),
                |p, n, o| worker_sandbox_class(op, p, n, o),
            )
            .unique_by(|c| c.sandbox_class.clone())
            .finish()
        }

        /// A worker's capacity or allocation.
        pub fn worker_resources(
            op: &Operation,
            p: &Path,
            n: &pb::WorkerResources,
            o: Option<&pb::WorkerResources>,
        ) -> ErrorList {
            rules::all([
                int(
                    op,
                    &p.child("cpu_millicores"),
                    n.cpu_millicores,
                    o.map(|o| o.cpu_millicores),
                )
                .optional()
                .min(0)
                .max(999_999)
                .finish(),
                int(
                    op,
                    &p.child("memory_bytes"),
                    n.memory_bytes,
                    o.map(|o| o.memory_bytes),
                )
                .optional()
                .min(0)
                .finish(),
                int(op, &p.child("actors"), n.actors, o.map(|o| o.actors))
                    .optional()
                    .min(1)
                    .finish(),
            ])
        }
    };
}

pub(crate) use workload_validators;
