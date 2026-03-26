use zksync_node_framework::{
    service::StopReceiver,
    task::{Task, TaskId, TaskKind},
    wiring_layer::{WiringError, WiringLayer},
    IntoContext,
};

/// Wiring layer that changes the handling of SIGTERM signal, preventing an immediate shutdown.
/// Instead, it would propagate the signal to the rest of the node, allowing it to shut down gracefully.
#[derive(Debug)]
pub struct SigtermHandlerLayer;

#[derive(Debug, IntoContext)]
pub struct Output {
    #[context(task)]
    pub task: SigtermHandlerTask,
}

#[async_trait::async_trait]
impl WiringLayer for SigtermHandlerLayer {
    type Input = ();
    type Output = Output;

    fn layer_name(&self) -> &'static str {
        "sigterm_handler_layer"
    }

    async fn wire(self, _input: Self::Input) -> Result<Self::Output, WiringError> {
        Ok(Output {
            task: SigtermHandlerTask,
        })
    }
}

#[derive(Debug)]
pub struct SigtermHandlerTask;

#[async_trait::async_trait]
impl Task for SigtermHandlerTask {
    fn kind(&self) -> TaskKind {
        // SIGTERM may happen at any time, so we must handle it as soon as it happens.
        TaskKind::UnconstrainedTask
    }

    fn id(&self) -> TaskId {
        "sigterm_handler".into()
    }

    async fn run(self: Box<Self>, mut stop_receiver: StopReceiver) -> anyhow::Result<()> {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate())?;

        // Wait for either SIGTERM or stop signal.
        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM signal");
            }
            _ = stop_receiver.0.changed() => {},
        }

        Ok(())
    }
}
