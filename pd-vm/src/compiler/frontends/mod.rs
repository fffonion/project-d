mod javascript;
mod lua;
mod rss;
mod scheme;

use super::{FunctionDecl, ParseError, Parser, SourceFlavor, Stmt};

pub(super) struct FrontendOutput {
    pub(super) stmts: Vec<Stmt>,
    pub(super) locals: usize,
    pub(super) local_bindings: Vec<(String, u8)>,
    pub(super) functions: Vec<FunctionDecl>,
}

trait FrontendCompiler {
    fn parse(&self, source: &str) -> Result<FrontendOutput, ParseError>;
}

struct RssCompiler;
struct JavaScriptCompiler;
struct LuaCompiler;
struct SchemeCompiler;

pub(super) fn parse_source(
    source: &str,
    flavor: SourceFlavor,
) -> Result<FrontendOutput, ParseError> {
    let frontend: &dyn FrontendCompiler = match flavor {
        SourceFlavor::Rss => &RssCompiler,
        SourceFlavor::JavaScript => &JavaScriptCompiler,
        SourceFlavor::Lua => &LuaCompiler,
        SourceFlavor::Scheme => &SchemeCompiler,
    };
    frontend.parse(source)
}

impl FrontendCompiler for RssCompiler {
    fn parse(&self, source: &str) -> Result<FrontendOutput, ParseError> {
        let lowered = rss::lower(source);
        parse_with_parser(&lowered, false)
    }
}

impl FrontendCompiler for JavaScriptCompiler {
    fn parse(&self, source: &str) -> Result<FrontendOutput, ParseError> {
        let lowered = javascript::lower(source)?;
        parse_with_parser(&lowered, true)
    }
}

impl FrontendCompiler for LuaCompiler {
    fn parse(&self, source: &str) -> Result<FrontendOutput, ParseError> {
        let lowered = lua::lower(source)?;
        parse_with_parser(&lowered, true)
    }
}

impl FrontendCompiler for SchemeCompiler {
    fn parse(&self, source: &str) -> Result<FrontendOutput, ParseError> {
        let lowered = scheme::lower(source)?;
        parse_with_parser(&lowered, true)
    }
}

fn parse_with_parser(
    source: &str,
    allow_implicit_externs: bool,
) -> Result<FrontendOutput, ParseError> {
    let mut parser = Parser::new(source, allow_implicit_externs)?;
    let stmts = parser.parse_program()?;
    Ok(FrontendOutput {
        stmts,
        locals: parser.local_count(),
        local_bindings: parser.local_bindings(),
        functions: parser.function_decls(),
    })
}
