#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AbiFunction {
    pub index: u16,
    pub name: &'static str,
    pub arity: u8,
}

pub const ABI_VERSION: u16 = 2;

pub const FN_GET_HEADER: u16 = 0;
pub const FN_SET_HEADER: u16 = 1;
pub const FN_SET_RESPONSE_CONTENT: u16 = 2;
pub const FN_SET_UPSTREAM: u16 = 3;
pub const FN_RATE_LIMIT_ALLOW: u16 = 4;

pub const FUNCTIONS: [AbiFunction; 5] = [
    AbiFunction {
        index: FN_GET_HEADER,
        name: "get_header",
        arity: 1,
    },
    AbiFunction {
        index: FN_SET_HEADER,
        name: "set_header",
        arity: 2,
    },
    AbiFunction {
        index: FN_SET_RESPONSE_CONTENT,
        name: "set_response_content",
        arity: 1,
    },
    AbiFunction {
        index: FN_SET_UPSTREAM,
        name: "set_upstream",
        arity: 1,
    },
    AbiFunction {
        index: FN_RATE_LIMIT_ALLOW,
        name: "rate_limit_allow",
        arity: 3,
    },
];

pub const HOST_FUNCTION_COUNT: u16 = FUNCTIONS.len() as u16;

fn functions_by_name() -> &'static std::collections::HashMap<&'static str, &'static AbiFunction> {
    static LOOKUP: std::sync::OnceLock<
        std::collections::HashMap<&'static str, &'static AbiFunction>,
    > = std::sync::OnceLock::new();
    LOOKUP.get_or_init(|| {
        let mut map = std::collections::HashMap::with_capacity(FUNCTIONS.len());
        for function in FUNCTIONS.iter() {
            map.insert(function.name, function);
        }
        map
    })
}

pub fn function_by_index(index: u16) -> Option<&'static AbiFunction> {
    FUNCTIONS.iter().find(|function| function.index == index)
}

pub fn function_by_name(name: &str) -> Option<&'static AbiFunction> {
    functions_by_name().get(name).copied()
}

pub fn abi_json() -> &'static str {
    include_str!("../abi.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn functions_are_dense_and_ordered() {
        for (position, function) in FUNCTIONS.iter().enumerate() {
            assert_eq!(function.index as usize, position);
        }
        assert_eq!(HOST_FUNCTION_COUNT as usize, FUNCTIONS.len());
    }

    #[test]
    fn abi_json_contains_declared_functions() {
        let manifest = abi_json();
        assert!(manifest.contains("\"abi_version\": 2"));
        for function in FUNCTIONS {
            assert!(manifest.contains(function.name));
        }
    }
}
