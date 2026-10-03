//! Types for unified addresses

use derive_new::new;
use getset::Getters;

/// `z_listunifiedreceivers` response
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct ZListUnifiedReceiversResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getset(get = "pub")]
    orchard: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getset(get = "pub")]
    sapling: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getset(get = "pub")]
    p2pkh: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getset(get = "pub")]
    p2sh: Option<String>,
}

impl Default for ZListUnifiedReceiversResponse {
    fn default() -> Self {
        Self {
            orchard: Some("orchard address if any".to_string()),
            sapling: Some("sapling address if any".to_string()),
            p2pkh: Some("p2pkh address if any".to_string()),
            p2sh: Some("p2sh address if any".to_string()),
        }
    }
}
