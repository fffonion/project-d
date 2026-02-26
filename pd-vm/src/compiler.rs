use std::collections::HashMap;
use std::path::Path;

use crate::assembler::{Assembler, AssemblerError};
use crate::vm::{HostImport, Program, Value, Vm};

#[derive(Debug)]
pub enum CompileError {
    Assembler(AssemblerError),
    CallArityOverflow,
    ClosureUsedAsValue,
    BreakOutsideLoop,
    ContinueOutsideLoop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub line: usize,
    pub message: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug)]
pub enum SourceError {
    Parse(ParseError),
    Compile(CompileError),
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceError::Parse(err) => write!(f, "{err}"),
            SourceError::Compile(err) => write!(f, "compile error: {err:?}"),
        }
    }
}

impl std::error::Error for SourceError {}

#[derive(Debug)]
pub enum SourcePathError {
    Io(std::io::Error),
    MissingExtension,
    UnsupportedExtension(String),
    Source(SourceError),
}

impl std::fmt::Display for SourcePathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourcePathError::Io(err) => write!(f, "{err}"),
            SourcePathError::MissingExtension => write!(f, "source file must have an extension"),
            SourcePathError::UnsupportedExtension(ext) => write!(
                f,
                "unsupported source extension '.{ext}', expected .rss, .js, .lua, or .scm"
            ),
            SourcePathError::Source(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for SourcePathError {}

impl From<std::io::Error> for SourcePathError {
    fn from(value: std::io::Error) -> Self {
        SourcePathError::Io(value)
    }
}

impl From<SourceError> for SourcePathError {
    fn from(value: SourceError) -> Self {
        SourcePathError::Source(value)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SourceFlavor {
    Rss,
    JavaScript,
    Lua,
    Scheme,
}

impl SourceFlavor {
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_ascii_lowercase().as_str() {
            "rss" => Some(Self::Rss),
            "js" => Some(Self::JavaScript),
            "lua" => Some(Self::Lua),
            "scm" => Some(Self::Scheme),
            _ => None,
        }
    }

    fn from_path(path: &Path) -> Result<Self, SourcePathError> {
        let ext = path
            .extension()
            .and_then(|value| value.to_str())
            .ok_or(SourcePathError::MissingExtension)?;
        SourceFlavor::from_extension(ext)
            .ok_or_else(|| SourcePathError::UnsupportedExtension(ext.to_string()))
    }
}

const STDLIB_PRINT_NAME: &str = "print";
const STDLIB_PRINT_ARITY: u8 = 1;

mod frontends;

#[derive(Clone, Debug)]
pub struct ClosureExpr {
    pub param_slots: Vec<u8>,
    pub capture_copies: Vec<(u8, u8)>,
    pub body: Box<Expr>,
}

#[derive(Clone, Debug)]
pub enum Expr {
    Int(i64),
    Bool(bool),
    String(String),
    Call(u16, Vec<Expr>),
    Closure(ClosureExpr),
    ClosureCall(ClosureExpr, Vec<Expr>),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Neg(Box<Expr>),
    Eq(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Var(u8),
}

#[derive(Clone, Debug)]
pub enum Stmt {
    Let {
        index: u8,
        expr: Expr,
        line: u32,
    },
    Assign {
        index: u8,
        expr: Expr,
        line: u32,
    },
    ClosureLet {
        line: u32,
        closure: ClosureExpr,
    },
    FuncDecl {
        name: String,
        arity: u8,
        args: Vec<String>,
        line: u32,
    },
    Expr {
        expr: Expr,
        line: u32,
    },
    IfElse {
        condition: Expr,
        then_branch: Vec<Stmt>,
        else_branch: Vec<Stmt>,
        line: u32,
    },
    For {
        init: Box<Stmt>,
        condition: Expr,
        post: Box<Stmt>,
        body: Vec<Stmt>,
        line: u32,
    },
    While {
        condition: Expr,
        body: Vec<Stmt>,
        line: u32,
    },
    Break {
        line: u32,
    },
    Continue {
        line: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionDecl {
    pub name: String,
    pub arity: u8,
    pub index: u16,
    pub args: Vec<String>,
}

pub struct CompiledProgram {
    pub program: Program,
    pub locals: usize,
    pub functions: Vec<FunctionDecl>,
}

impl CompiledProgram {
    pub fn into_vm(self) -> Vm {
        Vm::with_locals(self.program, self.locals)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum TokenKind {
    Ident(String),
    Int(i64),
    String(String),
    True,
    False,
    Fn,
    Let,
    For,
    If,
    Else,
    While,
    Break,
    Continue,
    Plus,
    Minus,
    Star,
    Slash,
    Pipe,
    LParen,
    RParen,
    LBrace,
    RBrace,
    Comma,
    Semicolon,
    Equal,
    EqualEqual,
    Less,
    Greater,
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
struct Token {
    kind: TokenKind,
    line: usize,
}

struct Lexer<'a> {
    chars: std::str::Chars<'a>,
    current: Option<char>,
    line: usize,
}

impl<'a> Lexer<'a> {
    fn new(source: &'a str) -> Self {
        let mut chars = source.chars();
        let current = chars.next();
        Self {
            chars,
            current,
            line: 1,
        }
    }

    fn next_token(&mut self) -> Result<Token, ParseError> {
        self.skip_whitespace_and_comments()?;
        let line = self.line;
        let Some(ch) = self.current else {
            return Ok(Token {
                kind: TokenKind::Eof,
                line,
            });
        };

        let token = match ch {
            '+' => {
                self.advance();
                TokenKind::Plus
            }
            '-' => {
                self.advance();
                TokenKind::Minus
            }
            '*' => {
                self.advance();
                TokenKind::Star
            }
            '/' => {
                self.advance();
                TokenKind::Slash
            }
            '|' => {
                self.advance();
                TokenKind::Pipe
            }
            '(' => {
                self.advance();
                TokenKind::LParen
            }
            ')' => {
                self.advance();
                TokenKind::RParen
            }
            '{' => {
                self.advance();
                TokenKind::LBrace
            }
            '}' => {
                self.advance();
                TokenKind::RBrace
            }
            ',' => {
                self.advance();
                TokenKind::Comma
            }
            ';' => {
                self.advance();
                TokenKind::Semicolon
            }
            '<' => {
                self.advance();
                TokenKind::Less
            }
            '>' => {
                self.advance();
                TokenKind::Greater
            }
            '=' => {
                self.advance();
                if self.current == Some('=') {
                    self.advance();
                    TokenKind::EqualEqual
                } else {
                    TokenKind::Equal
                }
            }
            '"' => {
                let value = self.consume_string()?;
                TokenKind::String(value)
            }
            c if c.is_ascii_digit() => {
                let value = self.consume_number()?;
                TokenKind::Int(value)
            }
            c if is_ident_start(c) => {
                let ident = self.consume_ident();
                match ident.as_str() {
                    "fn" => TokenKind::Fn,
                    "let" => TokenKind::Let,
                    "for" => TokenKind::For,
                    "if" => TokenKind::If,
                    "else" => TokenKind::Else,
                    "while" => TokenKind::While,
                    "break" => TokenKind::Break,
                    "continue" => TokenKind::Continue,
                    "true" => TokenKind::True,
                    "false" => TokenKind::False,
                    _ => TokenKind::Ident(ident),
                }
            }
            other => {
                return Err(ParseError {
                    line,
                    message: format!("unexpected character '{other}'"),
                });
            }
        };

        Ok(Token { kind: token, line })
    }

    fn advance(&mut self) {
        if self.current == Some('\n') {
            self.line += 1;
        }
        self.current = self.chars.next();
    }

    fn skip_whitespace_and_comments(&mut self) -> Result<(), ParseError> {
        loop {
            while matches!(self.current, Some(c) if c.is_whitespace()) {
                self.advance();
            }

            let mut peek = self.chars.clone();
            if self.current == Some('/') && peek.next() == Some('/') {
                while let Some(ch) = self.current {
                    self.advance();
                    if ch == '\n' {
                        break;
                    }
                }
                continue;
            }
            let mut peek = self.chars.clone();
            if self.current == Some('/') && peek.next() == Some('*') {
                let start_line = self.line;
                self.advance();
                self.advance();
                loop {
                    let Some(ch) = self.current else {
                        return Err(ParseError {
                            line: start_line,
                            message: "unterminated block comment".to_string(),
                        });
                    };
                    if ch == '*' {
                        let mut close = self.chars.clone();
                        if close.next() == Some('/') {
                            self.advance();
                            self.advance();
                            break;
                        }
                    }
                    self.advance();
                }
                continue;
            }
            break;
        }
        Ok(())
    }

    fn consume_number(&mut self) -> Result<i64, ParseError> {
        let line = self.line;
        let mut text = String::new();
        while let Some(ch) = self.current {
            if ch.is_ascii_digit() {
                text.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        text.parse::<i64>().map_err(|_| ParseError {
            line,
            message: format!("invalid number '{text}'"),
        })
    }

    fn consume_string(&mut self) -> Result<String, ParseError> {
        let line = self.line;
        if self.current != Some('"') {
            return Err(ParseError {
                line,
                message: "string literal must start with '\"'".to_string(),
            });
        }
        self.advance();

        let mut out = String::new();
        loop {
            let Some(ch) = self.current else {
                return Err(ParseError {
                    line,
                    message: "unterminated string literal".to_string(),
                });
            };

            match ch {
                '"' => {
                    self.advance();
                    break;
                }
                '\\' => {
                    self.advance();
                    let Some(escaped) = self.current else {
                        return Err(ParseError {
                            line,
                            message: "unterminated string escape".to_string(),
                        });
                    };
                    let mapped = match escaped {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '\\' => '\\',
                        '"' => '"',
                        '0' => '\0',
                        other => {
                            return Err(ParseError {
                                line,
                                message: format!("invalid escape '\\{other}'"),
                            });
                        }
                    };
                    out.push(mapped);
                    self.advance();
                }
                other => {
                    out.push(other);
                    self.advance();
                }
            }
        }

        Ok(out)
    }

    fn consume_ident(&mut self) -> String {
        let mut text = String::new();
        while let Some(ch) = self.current {
            if is_ident_continue(ch) {
                text.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        text
    }
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_ident_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    locals: HashMap<String, u8>,
    next_local: u8,
    functions: HashMap<String, FunctionDecl>,
    function_list: Vec<FunctionDecl>,
    next_function: u16,
    closure_bindings: HashMap<String, ClosureExpr>,
    closure_scopes: Vec<HashMap<String, u8>>,
    closure_capture_contexts: Vec<ClosureCaptureContext>,
    allow_implicit_externs: bool,
    loop_depth: usize,
}

struct ClosureCaptureContext {
    by_name: HashMap<String, u8>,
    capture_copies: Vec<(u8, u8)>,
}

impl Parser {
    fn new(source: &str, allow_implicit_externs: bool) -> Result<Self, ParseError> {
        let mut lexer = Lexer::new(source);
        let mut tokens = Vec::new();
        loop {
            let token = lexer.next_token()?;
            let is_eof = matches!(token.kind, TokenKind::Eof);
            tokens.push(token);
            if is_eof {
                break;
            }
        }
        Ok(Self {
            tokens,
            pos: 0,
            locals: HashMap::new(),
            next_local: 0,
            functions: HashMap::new(),
            function_list: Vec::new(),
            next_function: 0,
            closure_bindings: HashMap::new(),
            closure_scopes: Vec::new(),
            closure_capture_contexts: Vec::new(),
            allow_implicit_externs,
            loop_depth: 0,
        })
    }

    fn parse_program(&mut self) -> Result<Vec<Stmt>, ParseError> {
        let mut stmts = Vec::new();
        while !self.check(&TokenKind::Eof) {
            stmts.push(self.parse_stmt()?);
        }
        Ok(stmts)
    }

    fn parse_stmt(&mut self) -> Result<Stmt, ParseError> {
        if self.match_kind(&TokenKind::Fn) {
            return self.parse_fn_decl();
        }
        if self.match_kind(&TokenKind::Let) {
            return self.parse_let_with_terminator(true);
        }
        if self.match_kind(&TokenKind::For) {
            return self.parse_for();
        }
        if self.match_kind(&TokenKind::If) {
            return self.parse_if();
        }
        if self.match_kind(&TokenKind::While) {
            return self.parse_while();
        }
        if self.match_kind(&TokenKind::Break) {
            return self.parse_loop_control_stmt(true);
        }
        if self.match_kind(&TokenKind::Continue) {
            return self.parse_loop_control_stmt(false);
        }
        if self.check_assignment_start() {
            return self.parse_assign_with_terminator(true);
        }

        let line = self.current_line_u32();
        let expr = self.parse_expr()?;
        self.expect(&TokenKind::Semicolon, "expected ';' after expression")?;
        Ok(Stmt::Expr { expr, line })
    }

    fn parse_loop_control_stmt(&mut self, is_break: bool) -> Result<Stmt, ParseError> {
        let line = self.last_line();
        if self.loop_depth == 0 {
            return Err(ParseError {
                line: line as usize,
                message: if is_break {
                    "'break' is only allowed inside loops".to_string()
                } else {
                    "'continue' is only allowed inside loops".to_string()
                },
            });
        }
        self.expect(
            &TokenKind::Semicolon,
            if is_break {
                "expected ';' after break"
            } else {
                "expected ';' after continue"
            },
        )?;
        Ok(if is_break {
            Stmt::Break { line }
        } else {
            Stmt::Continue { line }
        })
    }

    fn parse_fn_decl(&mut self) -> Result<Stmt, ParseError> {
        let line = self.last_line();
        let name = self.expect_ident("expected function name after 'fn'")?;
        self.expect(&TokenKind::LParen, "expected '(' after function name")?;
        let mut params = Vec::new();
        if !self.check(&TokenKind::RParen) {
            loop {
                let param = self.expect_ident("expected parameter name")?;
                params.push(param);
                if self.match_kind(&TokenKind::Comma) {
                    continue;
                }
                break;
            }
        }
        self.expect(&TokenKind::RParen, "expected ')' after parameters")?;
        self.expect(
            &TokenKind::Semicolon,
            "expected ';' after function declaration",
        )?;

        let arity = u8::try_from(params.len()).map_err(|_| ParseError {
            line: self.current_line(),
            message: "function arity too large".to_string(),
        })?;
        if self.functions.contains_key(&name) {
            return Err(ParseError {
                line: self.current_line(),
                message: format!("duplicate function '{name}'"),
            });
        }
        if self.locals.contains_key(&name) || self.closure_bindings.contains_key(&name) {
            return Err(ParseError {
                line: self.current_line(),
                message: format!("name '{name}' already used by a local binding"),
            });
        }
        let index = self.next_function;
        self.next_function = self.next_function.checked_add(1).ok_or(ParseError {
            line: self.current_line(),
            message: "function index overflow".to_string(),
        })?;
        let decl = FunctionDecl {
            name: name.clone(),
            arity,
            index,
            args: params.clone(),
        };
        self.functions.insert(name.clone(), decl.clone());
        self.function_list.push(decl.clone());
        Ok(Stmt::FuncDecl {
            name,
            arity,
            args: params,
            line,
        })
    }

    fn parse_let_with_terminator(&mut self, expect_terminator: bool) -> Result<Stmt, ParseError> {
        let line = self.last_line();
        let name = self.expect_ident("expected identifier after 'let'")?;
        self.expect(&TokenKind::Equal, "expected '=' after identifier")?;
        let expr = self.parse_expr()?;
        if expect_terminator {
            self.expect(&TokenKind::Semicolon, "expected ';' after let")?;
        }

        if let Expr::Closure(closure) = expr {
            if self.locals.contains_key(&name)
                || self.functions.contains_key(&name)
                || self.closure_bindings.contains_key(&name)
            {
                return Err(ParseError {
                    line: self.current_line(),
                    message: format!("name '{name}' already used"),
                });
            }
            self.closure_bindings.insert(name, closure.clone());
            return Ok(Stmt::ClosureLet { line, closure });
        }

        if self.closure_bindings.contains_key(&name) {
            return Err(ParseError {
                line: self.current_line(),
                message: format!(
                    "cannot rebind closure '{name}' as a value variable in this compiler subset"
                ),
            });
        }

        let index = self.get_or_assign_local(&name)?;
        Ok(Stmt::Let { index, expr, line })
    }

    fn parse_assign_with_terminator(
        &mut self,
        expect_terminator: bool,
    ) -> Result<Stmt, ParseError> {
        let line = self.current_line_u32();
        let name = self.expect_ident("expected identifier before '='")?;
        self.expect(&TokenKind::Equal, "expected '=' after identifier")?;
        let expr = self.parse_expr()?;
        if expect_terminator {
            self.expect(&TokenKind::Semicolon, "expected ';' after assignment")?;
        }

        if self.closure_bindings.contains_key(&name) {
            return Err(ParseError {
                line: self.current_line(),
                message: format!("cannot assign to closure '{name}'"),
            });
        }

        let index = self.get_local(&name)?;
        Ok(Stmt::Assign { index, expr, line })
    }

    fn parse_for(&mut self) -> Result<Stmt, ParseError> {
        let line = self.last_line();
        self.expect(&TokenKind::LParen, "expected '(' after 'for'")?;

        let init = if self.match_kind(&TokenKind::Let) {
            self.parse_let_with_terminator(false)?
        } else if self.check_assignment_start() {
            self.parse_assign_with_terminator(false)?
        } else {
            let init_line = self.current_line_u32();
            let expr = self.parse_expr()?;
            Stmt::Expr {
                expr,
                line: init_line,
            }
        };
        self.expect(&TokenKind::Semicolon, "expected ';' after for initializer")?;

        let condition = self.parse_expr()?;
        self.expect(&TokenKind::Semicolon, "expected ';' after for condition")?;

        let post = if self.check_assignment_start() {
            self.parse_assign_with_terminator(false)?
        } else {
            let post_line = self.current_line_u32();
            let expr = self.parse_expr()?;
            Stmt::Expr {
                expr,
                line: post_line,
            }
        };
        self.expect(&TokenKind::RParen, "expected ')' after for clauses")?;
        self.loop_depth += 1;
        let body = self.parse_block("expected '{' after for clauses")?;
        self.loop_depth -= 1;
        Ok(Stmt::For {
            init: Box::new(init),
            condition,
            post: Box::new(post),
            body,
            line,
        })
    }

    fn parse_if(&mut self) -> Result<Stmt, ParseError> {
        let line = self.last_line();
        let condition = self.parse_expr()?;
        let then_branch = self.parse_block("expected '{' after if condition")?;
        let else_branch = if self.match_kind(&TokenKind::Else) {
            self.parse_block("expected '{' after else")?
        } else {
            Vec::new()
        };
        Ok(Stmt::IfElse {
            condition,
            then_branch,
            else_branch,
            line,
        })
    }

    fn parse_while(&mut self) -> Result<Stmt, ParseError> {
        let line = self.last_line();
        let condition = self.parse_expr()?;
        self.loop_depth += 1;
        let body = self.parse_block("expected '{' after while condition")?;
        self.loop_depth -= 1;
        Ok(Stmt::While {
            condition,
            body,
            line,
        })
    }

    fn parse_block(&mut self, message: &str) -> Result<Vec<Stmt>, ParseError> {
        self.expect(&TokenKind::LBrace, message)?;
        let mut stmts = Vec::new();
        while !self.check(&TokenKind::RBrace) {
            if self.check(&TokenKind::Eof) {
                return Err(ParseError {
                    line: self.current_line(),
                    message: "unexpected end of input in block".to_string(),
                });
            }
            stmts.push(self.parse_stmt()?);
        }
        self.expect(&TokenKind::RBrace, "expected '}' to close block")?;
        Ok(stmts)
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_term()?;
        loop {
            if self.match_kind(&TokenKind::EqualEqual) {
                let rhs = self.parse_term()?;
                expr = Expr::Eq(Box::new(expr), Box::new(rhs));
            } else if self.match_kind(&TokenKind::Less) {
                let rhs = self.parse_term()?;
                expr = Expr::Lt(Box::new(expr), Box::new(rhs));
            } else if self.match_kind(&TokenKind::Greater) {
                let rhs = self.parse_term()?;
                expr = Expr::Gt(Box::new(expr), Box::new(rhs));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_term(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_factor()?;
        loop {
            if self.match_kind(&TokenKind::Plus) {
                let rhs = self.parse_factor()?;
                expr = Expr::Add(Box::new(expr), Box::new(rhs));
            } else if self.match_kind(&TokenKind::Minus) {
                let rhs = self.parse_factor()?;
                expr = Expr::Sub(Box::new(expr), Box::new(rhs));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_factor(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_unary()?;
        loop {
            if self.match_kind(&TokenKind::Star) {
                let rhs = self.parse_unary()?;
                expr = Expr::Mul(Box::new(expr), Box::new(rhs));
            } else if self.match_kind(&TokenKind::Slash) {
                let rhs = self.parse_unary()?;
                expr = Expr::Div(Box::new(expr), Box::new(rhs));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        if self.match_kind(&TokenKind::Minus) {
            let inner = self.parse_unary()?;
            Ok(Expr::Neg(Box::new(inner)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        if self.match_kind(&TokenKind::True) {
            return Ok(Expr::Bool(true));
        }
        if self.match_kind(&TokenKind::False) {
            return Ok(Expr::Bool(false));
        }
        if let Some(value) = self.match_int() {
            return Ok(Expr::Int(value));
        }
        if let Some(value) = self.match_string() {
            return Ok(Expr::String(value));
        }
        if self.match_kind(&TokenKind::Pipe) {
            return self.parse_closure_literal();
        }
        if let Some(name) = self.match_ident() {
            if self.match_kind(&TokenKind::LParen) {
                let args = self.parse_call_args()?;
                if let Some(closure) = self.closure_bindings.get(&name).cloned() {
                    if closure.param_slots.len() != args.len() {
                        return Err(ParseError {
                            line: self.current_line(),
                            message: format!(
                                "closure '{name}' expects {} arguments",
                                closure.param_slots.len()
                            ),
                        });
                    }
                    return Ok(Expr::ClosureCall(closure, args));
                }
                let decl = self.resolve_function_for_call(&name, args.len())?;
                return Ok(Expr::Call(decl.index, args));
            }
            if self.closure_bindings.contains_key(&name) {
                return Err(ParseError {
                    line: self.current_line(),
                    message: format!("closure '{name}' must be called with '(...)'"),
                });
            }
            let index = self.get_local(&name)?;
            return Ok(Expr::Var(index));
        }
        if self.match_kind(&TokenKind::LParen) {
            let expr = self.parse_expr()?;
            self.expect(&TokenKind::RParen, "expected ')' after expression")?;
            return Ok(expr);
        }

        Err(ParseError {
            line: self.current_line(),
            message: "expected expression".to_string(),
        })
    }

    fn parse_closure_literal(&mut self) -> Result<Expr, ParseError> {
        let mut param_slots = Vec::new();
        let mut param_scope = HashMap::new();
        if !self.check(&TokenKind::Pipe) {
            loop {
                let param_name = self.expect_ident("expected closure parameter name")?;
                if param_scope.contains_key(&param_name) {
                    return Err(ParseError {
                        line: self.current_line(),
                        message: format!("duplicate closure parameter '{param_name}'"),
                    });
                }
                let slot = self.allocate_hidden_local()?;
                param_scope.insert(param_name, slot);
                param_slots.push(slot);
                if self.match_kind(&TokenKind::Comma) {
                    continue;
                }
                break;
            }
        }
        self.expect(&TokenKind::Pipe, "expected '|' after closure parameters")?;
        self.closure_scopes.push(param_scope);
        self.closure_capture_contexts.push(ClosureCaptureContext {
            by_name: HashMap::new(),
            capture_copies: Vec::new(),
        });
        let body = self.parse_expr()?;
        let capture_context = self
            .closure_capture_contexts
            .pop()
            .ok_or_else(|| ParseError {
                line: self.current_line(),
                message: "internal closure capture state error".to_string(),
            })?;
        self.closure_scopes.pop();
        Ok(Expr::Closure(ClosureExpr {
            param_slots,
            capture_copies: capture_context.capture_copies,
            body: Box::new(body),
        }))
    }

    fn parse_call_args(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut args = Vec::new();
        if !self.check(&TokenKind::RParen) {
            loop {
                args.push(self.parse_expr()?);
                if self.match_kind(&TokenKind::Comma) {
                    continue;
                }
                break;
            }
        }
        self.expect(&TokenKind::RParen, "expected ')' after arguments")?;
        Ok(args)
    }

    fn expect(&mut self, kind: &TokenKind, message: &str) -> Result<(), ParseError> {
        if self.match_kind(kind) {
            Ok(())
        } else {
            Err(ParseError {
                line: self.current_line(),
                message: message.to_string(),
            })
        }
    }

    fn expect_ident(&mut self, message: &str) -> Result<String, ParseError> {
        if let Some(name) = self.match_ident() {
            Ok(name)
        } else {
            Err(ParseError {
                line: self.current_line(),
                message: message.to_string(),
            })
        }
    }

    fn get_local(&mut self, name: &str) -> Result<u8, ParseError> {
        for scope in self.closure_scopes.iter().rev() {
            if let Some(&index) = scope.get(name) {
                return Ok(index);
            }
        }
        if let Some(source_index) = self.locals.get(name).copied() {
            if let Some(capture_idx) = self.closure_capture_contexts.len().checked_sub(1) {
                if let Some(&captured_slot) =
                    self.closure_capture_contexts[capture_idx].by_name.get(name)
                {
                    return Ok(captured_slot);
                }
                let captured_slot = self.allocate_hidden_local()?;
                self.closure_capture_contexts[capture_idx]
                    .by_name
                    .insert(name.to_string(), captured_slot);
                self.closure_capture_contexts[capture_idx]
                    .capture_copies
                    .push((source_index, captured_slot));
                return Ok(captured_slot);
            }
            return Ok(source_index);
        }
        Err(ParseError {
            line: self.current_line(),
            message: format!("unknown local '{name}'"),
        })
    }

    fn resolve_function_for_call(
        &mut self,
        name: &str,
        arg_count: usize,
    ) -> Result<FunctionDecl, ParseError> {
        if let Some(decl) = self.functions.get(name).cloned() {
            if decl.arity as usize != arg_count {
                return Err(ParseError {
                    line: self.current_line(),
                    message: format!("function '{name}' expects {} arguments", decl.arity),
                });
            }
            return Ok(decl);
        }

        if name == STDLIB_PRINT_NAME {
            let arg_arity = u8::try_from(arg_count).map_err(|_| ParseError {
                line: self.current_line(),
                message: "function arity too large".to_string(),
            })?;
            if arg_arity != STDLIB_PRINT_ARITY {
                return Err(ParseError {
                    line: self.current_line(),
                    message: format!(
                        "function '{STDLIB_PRINT_NAME}' expects {STDLIB_PRINT_ARITY} arguments"
                    ),
                });
            }
            return self.define_builtin_function(STDLIB_PRINT_NAME, STDLIB_PRINT_ARITY);
        }

        if self.allow_implicit_externs {
            let arity = u8::try_from(arg_count).map_err(|_| ParseError {
                line: self.current_line(),
                message: "function arity too large".to_string(),
            })?;
            return self.define_external_function(name, arity);
        }

        Err(ParseError {
            line: self.current_line(),
            message: format!("unknown function '{name}'"),
        })
    }

    fn define_builtin_function(
        &mut self,
        name: &str,
        arity: u8,
    ) -> Result<FunctionDecl, ParseError> {
        if let Some(existing) = self.functions.get(name) {
            return Ok(existing.clone());
        }
        if self.locals.contains_key(name) || self.closure_bindings.contains_key(name) {
            return Err(ParseError {
                line: self.current_line(),
                message: format!("name '{name}' already used by a local binding"),
            });
        }
        let index = self.next_function;
        self.next_function = self.next_function.checked_add(1).ok_or(ParseError {
            line: self.current_line(),
            message: "function index overflow".to_string(),
        })?;
        let decl = FunctionDecl {
            name: name.to_string(),
            arity,
            index,
            args: vec!["value".to_string()],
        };
        self.functions.insert(name.to_string(), decl.clone());
        self.function_list.push(decl.clone());
        Ok(decl)
    }

    fn define_external_function(
        &mut self,
        name: &str,
        arity: u8,
    ) -> Result<FunctionDecl, ParseError> {
        if let Some(existing) = self.functions.get(name) {
            if existing.arity != arity {
                return Err(ParseError {
                    line: self.current_line(),
                    message: format!("function '{name}' expects {} arguments", existing.arity),
                });
            }
            return Ok(existing.clone());
        }
        if self.locals.contains_key(name) || self.closure_bindings.contains_key(name) {
            return Err(ParseError {
                line: self.current_line(),
                message: format!("name '{name}' already used by a local binding"),
            });
        }
        let index = self.next_function;
        self.next_function = self.next_function.checked_add(1).ok_or(ParseError {
            line: self.current_line(),
            message: "function index overflow".to_string(),
        })?;
        let args = (0..arity).map(|idx| format!("arg{idx}")).collect();
        let decl = FunctionDecl {
            name: name.to_string(),
            arity,
            index,
            args,
        };
        self.functions.insert(name.to_string(), decl.clone());
        self.function_list.push(decl.clone());
        Ok(decl)
    }

    fn get_or_assign_local(&mut self, name: &str) -> Result<u8, ParseError> {
        if let Some(&index) = self.locals.get(name) {
            return Ok(index);
        }
        let index = self.allocate_hidden_local()?;
        self.locals.insert(name.to_string(), index);
        Ok(index)
    }

    fn allocate_hidden_local(&mut self) -> Result<u8, ParseError> {
        let index = self.next_local;
        self.next_local = self.next_local.checked_add(1).ok_or(ParseError {
            line: self.current_line(),
            message: "local index overflow".to_string(),
        })?;
        Ok(index)
    }

    fn match_kind(&mut self, kind: &TokenKind) -> bool {
        if self.check(kind) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn match_int(&mut self) -> Option<i64> {
        match self.tokens.get(self.pos) {
            Some(Token {
                kind: TokenKind::Int(value),
                ..
            }) => {
                let value = *value;
                self.pos += 1;
                Some(value)
            }
            _ => None,
        }
    }

    fn match_ident(&mut self) -> Option<String> {
        match self.tokens.get(self.pos) {
            Some(Token {
                kind: TokenKind::Ident(name),
                ..
            }) => {
                let name = name.clone();
                self.pos += 1;
                Some(name)
            }
            _ => None,
        }
    }

    fn match_string(&mut self) -> Option<String> {
        match self.tokens.get(self.pos) {
            Some(Token {
                kind: TokenKind::String(value),
                ..
            }) => {
                let value = value.clone();
                self.pos += 1;
                Some(value)
            }
            _ => None,
        }
    }

    fn check_assignment_start(&self) -> bool {
        matches!(
            (self.tokens.get(self.pos), self.tokens.get(self.pos + 1)),
            (
                Some(Token {
                    kind: TokenKind::Ident(_),
                    ..
                }),
                Some(Token {
                    kind: TokenKind::Equal,
                    ..
                })
            )
        )
    }

    fn check(&self, kind: &TokenKind) -> bool {
        matches!(self.peek_kind(), Some(k) if std::mem::discriminant(k) == std::mem::discriminant(kind))
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.tokens.get(self.pos).map(|token| &token.kind)
    }

    fn current_line(&self) -> usize {
        self.tokens
            .get(self.pos)
            .map(|token| token.line)
            .unwrap_or(1)
    }

    fn current_line_u32(&self) -> u32 {
        u32::try_from(self.current_line()).unwrap_or(u32::MAX)
    }

    fn last_line(&self) -> u32 {
        self.tokens
            .get(self.pos.saturating_sub(1))
            .map(|token| token.line)
            .unwrap_or(1) as u32
    }

    fn local_count(&self) -> usize {
        self.next_local as usize
    }

    fn function_decls(&self) -> Vec<FunctionDecl> {
        self.function_list.clone()
    }

    fn local_bindings(&self) -> Vec<(String, u8)> {
        let mut locals: Vec<(String, u8)> = self
            .locals
            .iter()
            .map(|(name, index)| (name.clone(), *index))
            .collect();
        locals.sort_by_key(|(_, index)| *index);
        locals
    }
}

pub fn compile_source(source: &str) -> Result<CompiledProgram, SourceError> {
    compile_source_with_flavor(source, SourceFlavor::Rss)
}

pub fn compile_source_with_flavor(
    source: &str,
    flavor: SourceFlavor,
) -> Result<CompiledProgram, SourceError> {
    let parsed = frontends::parse_source(source, flavor).map_err(SourceError::Parse)?;
    let mut compiler = Compiler::new();
    compiler.set_source(source.to_string());
    for func in &parsed.functions {
        compiler.add_function_debug(func);
    }
    for (name, index) in parsed.local_bindings {
        compiler.add_local_debug(name, index);
    }
    let mut program = compiler
        .compile_program(&parsed.stmts)
        .map_err(SourceError::Compile)?;
    program.imports = parsed
        .functions
        .iter()
        .map(|func| HostImport {
            name: func.name.clone(),
            arity: func.arity,
        })
        .collect();
    Ok(CompiledProgram {
        program,
        locals: parsed.locals,
        functions: parsed.functions,
    })
}

pub fn compile_source_file(path: impl AsRef<Path>) -> Result<CompiledProgram, SourcePathError> {
    let path = path.as_ref();
    let flavor = SourceFlavor::from_path(path)?;
    let source = std::fs::read_to_string(path)?;
    compile_source_with_flavor(&source, flavor).map_err(SourcePathError::Source)
}

pub struct Compiler {
    assembler: Assembler,
    next_label_id: u32,
    loop_stack: Vec<LoopContext>,
}

struct LoopContext {
    continue_label: String,
    break_label: String,
}

impl Default for Compiler {
    fn default() -> Self {
        Self::new()
    }
}

impl Compiler {
    pub fn new() -> Self {
        Self {
            assembler: Assembler::new(),
            next_label_id: 0,
            loop_stack: Vec::new(),
        }
    }

    pub fn set_source(&mut self, source: String) {
        self.assembler.set_source(source);
    }

    pub fn add_function_debug(&mut self, func: &FunctionDecl) {
        self.assembler
            .add_function(func.name.clone(), func.args.clone());
    }

    pub fn add_local_debug(&mut self, name: String, index: u8) {
        self.assembler.add_local(name, index);
    }

    pub fn compile_program(mut self, stmts: &[Stmt]) -> Result<Program, CompileError> {
        self.compile_stmts(stmts)?;
        self.assembler.ret();
        self.assembler
            .finish_program()
            .map_err(CompileError::Assembler)
    }

    fn compile_stmts(&mut self, stmts: &[Stmt]) -> Result<(), CompileError> {
        for stmt in stmts {
            self.compile_stmt(stmt)?;
        }
        Ok(())
    }

    fn compile_stmt(&mut self, stmt: &Stmt) -> Result<(), CompileError> {
        match stmt {
            Stmt::Let { index, expr, line } => {
                self.assembler.mark_line(*line);
                self.compile_expr(expr)?;
                self.assembler.stloc(*index);
            }
            Stmt::Assign { index, expr, line } => {
                self.assembler.mark_line(*line);
                self.compile_expr(expr)?;
                self.assembler.stloc(*index);
            }
            Stmt::ClosureLet { line, closure } => {
                self.assembler.mark_line(*line);
                for (source_index, captured_slot) in &closure.capture_copies {
                    self.assembler.ldloc(*source_index);
                    self.assembler.stloc(*captured_slot);
                }
            }
            Stmt::FuncDecl { .. } => {}
            Stmt::Expr { expr, line } => {
                self.assembler.mark_line(*line);
                self.compile_expr(expr)?;
            }
            Stmt::IfElse {
                condition,
                then_branch,
                else_branch,
                line,
            } => {
                self.assembler.mark_line(*line);
                let else_label = self.fresh_label("else");
                let end_label = self.fresh_label("endif");
                self.compile_expr(condition)?;
                self.assembler.brfalse_label(&else_label);
                self.compile_stmts(then_branch)?;
                self.assembler.br_label(&end_label);
                self.assembler
                    .label(&else_label)
                    .map_err(CompileError::Assembler)?;
                self.compile_stmts(else_branch)?;
                self.assembler
                    .label(&end_label)
                    .map_err(CompileError::Assembler)?;
            }
            Stmt::For {
                init,
                condition,
                post,
                body,
                line,
            } => {
                self.assembler.mark_line(*line);
                self.compile_stmt(init)?;
                let start_label = self.fresh_label("for_start");
                let continue_label = self.fresh_label("for_continue");
                let end_label = self.fresh_label("for_end");
                self.assembler
                    .label(&start_label)
                    .map_err(CompileError::Assembler)?;
                self.compile_expr(condition)?;
                self.assembler.brfalse_label(&end_label);
                self.loop_stack.push(LoopContext {
                    continue_label: continue_label.clone(),
                    break_label: end_label.clone(),
                });
                self.compile_stmts(body)?;
                self.loop_stack.pop();
                self.assembler
                    .label(&continue_label)
                    .map_err(CompileError::Assembler)?;
                self.compile_stmt(post)?;
                self.assembler.br_label(&start_label);
                self.assembler
                    .label(&end_label)
                    .map_err(CompileError::Assembler)?;
            }
            Stmt::While {
                condition,
                body,
                line,
            } => {
                self.assembler.mark_line(*line);
                let start_label = self.fresh_label("while_start");
                let end_label = self.fresh_label("while_end");
                self.assembler
                    .label(&start_label)
                    .map_err(CompileError::Assembler)?;
                self.compile_expr(condition)?;
                self.assembler.brfalse_label(&end_label);
                self.loop_stack.push(LoopContext {
                    continue_label: start_label.clone(),
                    break_label: end_label.clone(),
                });
                self.compile_stmts(body)?;
                self.loop_stack.pop();
                self.assembler.br_label(&start_label);
                self.assembler
                    .label(&end_label)
                    .map_err(CompileError::Assembler)?;
            }
            Stmt::Break { line } => {
                self.assembler.mark_line(*line);
                let loop_ctx = self
                    .loop_stack
                    .last()
                    .ok_or(CompileError::BreakOutsideLoop)?;
                self.assembler.br_label(&loop_ctx.break_label);
            }
            Stmt::Continue { line } => {
                self.assembler.mark_line(*line);
                let loop_ctx = self
                    .loop_stack
                    .last()
                    .ok_or(CompileError::ContinueOutsideLoop)?;
                self.assembler.br_label(&loop_ctx.continue_label);
            }
        }
        Ok(())
    }

    fn compile_expr(&mut self, expr: &Expr) -> Result<(), CompileError> {
        match expr {
            Expr::Int(value) => {
                self.assembler.push_const(Value::Int(*value));
            }
            Expr::Bool(value) => {
                self.assembler.push_const(Value::Bool(*value));
            }
            Expr::String(value) => {
                self.assembler.push_const(Value::String(value.clone()));
            }
            Expr::Call(index, args) => {
                for arg in args {
                    self.compile_expr(arg)?;
                }
                let argc = u8::try_from(args.len()).map_err(|_| CompileError::CallArityOverflow)?;
                self.assembler.call(*index, argc);
            }
            Expr::Closure(_) => {
                return Err(CompileError::ClosureUsedAsValue);
            }
            Expr::ClosureCall(closure, args) => {
                for (arg, slot) in args.iter().zip(closure.param_slots.iter()) {
                    self.compile_expr(arg)?;
                    self.assembler.stloc(*slot);
                }
                self.compile_expr(&closure.body)?;
            }
            Expr::Add(lhs, rhs) => {
                self.compile_expr(lhs)?;
                self.compile_expr(rhs)?;
                self.assembler.add();
            }
            Expr::Sub(lhs, rhs) => {
                self.compile_expr(lhs)?;
                self.compile_expr(rhs)?;
                self.assembler.sub();
            }
            Expr::Mul(lhs, rhs) => {
                if let Expr::Int(value) = rhs.as_ref()
                    && let Some(shift) = shift_amount_for_power_of_two(*value)
                {
                    self.compile_expr(lhs)?;
                    self.assembler.push_const(Value::Int(shift as i64));
                    self.assembler.shl();
                } else if let Expr::Int(value) = lhs.as_ref()
                    && let Some(shift) = shift_amount_for_power_of_two(*value)
                {
                    self.compile_expr(rhs)?;
                    self.assembler.push_const(Value::Int(shift as i64));
                    self.assembler.shl();
                } else {
                    self.compile_expr(lhs)?;
                    self.compile_expr(rhs)?;
                    self.assembler.mul();
                }
            }
            Expr::Div(lhs, rhs) => {
                self.compile_expr(lhs)?;
                self.compile_expr(rhs)?;
                self.assembler.div();
            }
            Expr::Neg(inner) => {
                self.compile_expr(inner)?;
                self.assembler.neg();
            }
            Expr::Eq(lhs, rhs) => {
                self.compile_expr(lhs)?;
                self.compile_expr(rhs)?;
                self.assembler.ceq();
            }
            Expr::Lt(lhs, rhs) => {
                self.compile_expr(lhs)?;
                self.compile_expr(rhs)?;
                self.assembler.clt();
            }
            Expr::Gt(lhs, rhs) => {
                self.compile_expr(lhs)?;
                self.compile_expr(rhs)?;
                self.assembler.cgt();
            }
            Expr::Var(index) => {
                self.assembler.ldloc(*index);
            }
        }
        Ok(())
    }

    fn fresh_label(&mut self, prefix: &str) -> String {
        let label = format!("{prefix}_{}", self.next_label_id);
        self.next_label_id += 1;
        label
    }
}

fn shift_amount_for_power_of_two(value: i64) -> Option<u32> {
    if value <= 0 {
        return None;
    }
    let as_u64 = value as u64;
    if !as_u64.is_power_of_two() {
        return None;
    }
    Some(as_u64.trailing_zeros())
}
