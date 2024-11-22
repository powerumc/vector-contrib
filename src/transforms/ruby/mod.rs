use crate::config::{ComponentKey, DataType, Input, LogNamespace, OutputId, TransformConfig, TransformContext, TransformOutput};
use crate::schema::Definition;
use crate::transforms::Transform;
use vector_config::NamedComponent;
use vector_config_macros::configurable_component;
use vector_lib::enrichment::TableRegistry;
use vector_lib::event::Event;
use vector_lib::transform::runtime_transform::RuntimeTransform;

/// Ruby transform configuration.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct RubyConfig {
    /// The Ruby source code to execute.
    #[configurable(derived)]
    source: Option<String>,
}

impl RubyConfig {
    pub fn build(&self, key: ComponentKey) -> crate::Result<Transform> {
        Ruby::new(self, key).map(Transform::event_task)
    }
}

/// Configuration for the `ruby` transform.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
struct HooksConfig {
    /// The Ruby source code to execute.
    #[configurable(derived)]
    process: String,
}

#[derive(Clone)]
pub struct Ruby {
    // ruby: Arc<Mutex<magnus::Ruby>>,
}

unsafe impl Send for Ruby {}

impl NamedComponent for RubyConfig {
    fn get_component_name(&self) -> &'static str {
        "ruby"
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "ruby")]
impl TransformConfig for RubyConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        let key = context
            .key
            .as_ref()
            .map_or_else(|| ComponentKey::from("ruby"), Clone::clone);

        self.build(key)
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(&self, _enrichment_tables: TableRegistry, input_definitions: &[(OutputId, Definition)], _global_log_namespace: LogNamespace) -> Vec<TransformOutput> {
        let namespaces = input_definitions
            .iter()
            .flat_map(|(_output, definition)| definition.log_namespaces().clone())
            .collect();

        let definition = input_definitions
            .iter()
            .map(|(output, _definition)| {
                (
                    output.clone(),
                    Definition::default_for_namespace(&namespaces),
                )
            })
            .collect();

        vec![TransformOutput::new(
            DataType::Metric | DataType::Log,
            definition,
        )]
    }
}

impl Ruby {
    pub fn new(config: &RubyConfig, _key: ComponentKey) -> crate::Result<Self> {
        if let Some(source) = &config.source {
            let ruby = unsafe { magnus::embed::setup() };
            let _: magnus::Value = ruby.eval(source).unwrap();
        }

        Ok(Self {
            // ruby: Arc::new(Mutex::new(ruby))
        })
    }
}

impl RuntimeTransform for Ruby {
    fn hook_process<F>(&mut self, _event: Event, _emit_fn: F)
    where
        F: FnMut(Event),
    {}
}