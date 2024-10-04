#[derive(Debug)]
pub struct InvalidEnumVariant;

macro_rules! int_enum {
    ($name:ident, $type:ty, $($variant:ident = $value:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($variant = $value),+
        }

        impl TryFrom<$type> for $name {
            type Error = InvalidEnumVariant;

            fn try_from(value: $type) -> Result<Self, Self::Error> {
                match value {
                    $($value => Ok($name::$variant),)*
                    _ => Err(InvalidEnumVariant),
                }
            }
        }
    }
}

int_enum!(KafkaApiKey, u16, Fetch = 1, ApiVersions = 18);
int_enum!(KafkaError, u16, NoError = 0, UnsupportedVersion = 35, UnknownTopic = 100);

#[allow(dead_code)] // Suppressed dead code warning
pub const VERSIONS: &'static [(KafkaApiKey, u16, u16)] = &[
    (KafkaApiKey::Fetch, 16, 16),
    (KafkaApiKey::ApiVersions, 4, 4),
];