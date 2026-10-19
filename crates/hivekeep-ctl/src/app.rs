//! Dispatch from the parsed command line to the commands.

use crate::cli::*;
use crate::commands::worker::Transition;
use crate::commands::*;
use crate::context::Ctx;
use crate::error::{CliError, Result};
use crate::io::Io;
use clap::CommandFactory;

/// The usage line of the deepest subcommand `argv` names, as clap renders
/// it (`Usage: hivectl create actor [OPTIONS] ...`).
pub fn usage_for(argv: &[String]) -> String {
    let mut root = Cli::command();
    root.build();
    let mut cmd = &mut root;
    for arg in argv.iter().skip(1) {
        let next = cmd
            .get_subcommands()
            .find(|c| c.get_name() == arg || c.get_all_aliases().any(|a| a == arg))
            .map(|c| c.get_name().to_string());
        if let Some(name) = next {
            cmd = cmd
                .find_subcommand_mut(&name)
                .expect("subcommand was just found");
        }
    }
    cmd.render_usage().to_string()
}

/// What `hivectl` writes for a command line clap rejected, or for help and
/// version requests. Every rejection carries the usage of the command that
/// was run, which clap leaves out of some of its messages.
pub fn parse_failure_report(err: &clap::Error, argv: &[String]) -> String {
    let text = err.render().to_string();
    if !err.use_stderr() || text.contains("Usage: ") {
        return text;
    }
    let usage = usage_for(argv);
    let hint = "\nFor more information, try '--help'.";
    match text.find(hint) {
        Some(at) => format!("{}\n{usage}\n{}", &text[..at], &text[at..]),
        None => format!("{}\n{usage}\n", text.trim_end()),
    }
}

/// What `hivectl` writes to stderr when `err` ends a run of `argv`: the
/// message, and for a usage error the usage of the command that was run.
/// A runtime failure prints no usage, since the command line was fine.
pub fn failure_report(err: &CliError, argv: &[String]) -> String {
    if err.is_usage() {
        format!(
            "Error: {}\n\n{}\n\nFor more information, try '--help'.\n",
            err.message(),
            usage_for(argv)
        )
    } else {
        format!("Error: {}\n", err.message())
    }
}

/// Runs a parsed command line against `io`.
pub async fn run(cli: Cli, io: Io) -> Result<()> {
    let mut ctx = Ctx::new(cli.global, io);
    let ctx = &mut ctx;
    match cli.command {
        Command::Get(get) => match get {
            GetCommand::Actor { names, scope } => {
                actor::get(ctx, &names, scope.hive.as_deref(), scope.all_hives).await
            }
            GetCommand::ActorTemplate { names, scope } => {
                actor_template::get(ctx, &names, scope.hive.as_deref(), scope.all_hives).await
            }
            GetCommand::Hive { names } => hive::get(ctx, &names).await,
            GetCommand::Tag { names, scope } => {
                tag::get(ctx, &names, scope.hive.as_deref(), scope.all_hives).await
            }
            GetCommand::Worker { names, filter } => worker::get(ctx, &names, &filter).await,
            GetCommand::WorkerAssignments { worker } => worker::assignments(ctx, &worker).await,
            GetCommand::EgressPolicy(t) => egress_policy::get(ctx, &t.name, &t.hive).await,
            GetCommand::AccessPolicy { scope } => {
                access_policy::get(ctx, scope.hive.as_deref(), scope.global).await
            }
        },
        Command::Create(create) => match create {
            CreateCommand::Actor {
                name,
                hive,
                template,
                tag,
            } => actor::create(ctx, &name, &hive, &template, tag.as_deref()).await,
            CreateCommand::ActorTemplate { manifest } => {
                actor_template::create(ctx, &manifest.filename).await
            }
            CreateCommand::Hive { name } => hive::create(ctx, &name).await,
            CreateCommand::Tag {
                name,
                hive,
                actor,
                scope,
            } => tag::create(ctx, &name, &hive, &actor, &scope).await,
            CreateCommand::EgressPolicy { target, manifest } => {
                egress_policy::create(ctx, &target.name, &target.hive, &manifest.filename).await
            }
            CreateCommand::AccessPolicy { scope, manifest } => {
                access_policy::create(ctx, scope.hive.as_deref(), scope.global, &manifest.filename)
                    .await
            }
        },
        Command::Update(update) => match update {
            UpdateCommand::Actor {
                target,
                template,
                worker_selector,
            } => {
                actor::update(
                    ctx,
                    &target.name,
                    &target.hive,
                    template.as_deref(),
                    worker_selector.as_deref(),
                )
                .await
            }
            UpdateCommand::Tag { target, scope } => {
                tag::update(ctx, &target.name, &target.hive, &scope).await
            }
            UpdateCommand::EgressPolicy { target, manifest } => {
                egress_policy::update(ctx, &target.name, &target.hive, &manifest.filename).await
            }
            UpdateCommand::AccessPolicy { scope, manifest } => {
                access_policy::update(ctx, scope.hive.as_deref(), scope.global, &manifest.filename)
                    .await
            }
        },
        Command::Delete(delete) => match delete {
            DeleteCommand::Actor(t) => actor::delete(ctx, &t.name, &t.hive).await,
            DeleteCommand::ActorTemplate(t) => actor_template::delete(ctx, &t.name, &t.hive).await,
            DeleteCommand::Hive { name } => hive::delete(ctx, &name).await,
            DeleteCommand::Tag(t) => tag::delete(ctx, &t.name, &t.hive).await,
            DeleteCommand::EgressPolicy { target, guards } => {
                egress_policy::delete(ctx, &target.name, &target.hive, &guards).await
            }
            DeleteCommand::AccessPolicy { hive, guards } => {
                access_policy::delete(ctx, &hive, &guards).await
            }
        },
        Command::Suspend(SuspendCommand::Actor { target, mode }) => {
            actor::suspend(ctx, &target.name, &target.hive, &mode).await
        }
        Command::Resume(ResumeCommand::Actor(t)) => actor::resume(ctx, &t.name, &t.hive).await,
        Command::Revert(RevertCommand::Actor(t)) => actor::revert(ctx, &t.name, &t.hive).await,
        Command::Logs(LogsCommand::Actor {
            target,
            follow,
            container,
            source,
        }) => logs::run(ctx, &target.name, &target.hive, follow, &container, &source).await,
        Command::Top(TopCommand::Worker { filter }) => worker_top::run(ctx, &filter).await,
        Command::Drain(DrainCommand::Worker(w)) => {
            worker::transition(ctx, &w.name, Transition::Drain).await
        }
        Command::MarkAvailable(MarkAvailableCommand::Worker(w)) => {
            worker::transition(ctx, &w.name, Transition::MarkAvailable).await
        }
        Command::MarkUnavailable(MarkUnavailableCommand::Worker(w)) => {
            worker::transition(ctx, &w.name, Transition::MarkUnavailable).await
        }
        Command::Proxy(ProxyCommand::Actor {
            name,
            ports,
            hive,
            address,
            gate,
        }) => proxy::run(ctx, &name, &hive, &ports, &address, &gate).await,
        Command::Admin(cmd) => admin::run(ctx, &cmd),
    }
}
