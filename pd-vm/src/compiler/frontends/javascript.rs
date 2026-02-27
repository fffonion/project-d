use super::super::{ParseError, STDLIB_PRINT_NAME};
use super::{is_ident_continue, is_ident_start};

pub(super) fn lower(source: &str) -> Result<String, ParseError> {
    let console_rewritten = rewrite_console_log_calls(source);
    let keyword_rewritten = rewrite_keywords(&console_rewritten, |ident| match ident {
        "function" => Some("fn"),
        "const" => Some("let"),
        _ => None,
    });

    let mut lines = Vec::new();
    let mut in_import_block = false;
    for (index, raw_line) in keyword_rewritten.lines().enumerate() {
        let line_no = index + 1;
        let trimmed = raw_line.trim();
        if in_import_block {
            lines.push(String::new());
            if trimmed.contains(" from ") || trimmed.ends_with(';') {
                in_import_block = false;
            }
            continue;
        }
        if trimmed.starts_with("import ") {
            lines.push(String::new());
            if !trimmed.contains(" from ") && !trimmed.ends_with(';') {
                in_import_block = true;
            }
            continue;
        }
        if is_js_external_decl_line(raw_line) {
            lines.push(String::new());
            continue;
        }
        lines.push(rewrite_js_arrow_line(raw_line, line_no)?);
    }
    Ok(lines.join("\n"))
}

fn is_js_external_decl_line(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return false;
    }

    if trimmed.starts_with("import ") {
        return true;
    }

    if !(trimmed.starts_with("let ")
        || trimmed.starts_with("const ")
        || trimmed.starts_with("var "))
    {
        return false;
    }

    trimmed.contains("require(")
}

fn rewrite_keywords<F>(source: &str, mut rewrite: F) -> String
where
    F: FnMut(&str) -> Option<&'static str>,
{
    let mut out = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    let mut in_string = false;
    let mut escaped = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    while let Some(ch) = chars.next() {
        if in_line_comment {
            out.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            out.push(ch);
            if ch == '*' && chars.peek().copied() == Some('/') {
                out.push('/');
                let _ = chars.next();
                in_block_comment = false;
            }
            continue;
        }

        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '/' {
            if chars.peek().copied() == Some('/') {
                out.push('/');
                out.push('/');
                let _ = chars.next();
                in_line_comment = true;
                continue;
            }
            if chars.peek().copied() == Some('*') {
                out.push('/');
                out.push('*');
                let _ = chars.next();
                in_block_comment = true;
                continue;
            }
        }

        if ch == '"' {
            in_string = true;
            out.push(ch);
            continue;
        }

        if is_ident_start(ch) {
            let mut ident = String::new();
            ident.push(ch);
            while let Some(next) = chars.peek().copied() {
                if is_ident_continue(next) {
                    ident.push(next);
                    let _ = chars.next();
                } else {
                    break;
                }
            }
            if let Some(value) = rewrite(&ident) {
                out.push_str(value);
            } else {
                out.push_str(&ident);
            }
        } else {
            out.push(ch);
        }
    }
    out
}

fn rewrite_console_log_calls(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut out = String::with_capacity(source.len());
    let mut i = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    const CONSOLE_DOT_LOG: &[u8] = b"console.log";

    while i < bytes.len() {
        let b = bytes[i];

        if in_block_comment {
            out.push(b as char);
            if b == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                out.push('/');
                i += 2;
                in_block_comment = false;
                continue;
            }
            i += 1;
            continue;
        }

        if in_line_comment {
            out.push(b as char);
            if b == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        if in_string {
            out.push(b as char);
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            out.push('/');
            out.push('/');
            i += 2;
            in_line_comment = true;
            continue;
        }

        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            out.push('/');
            out.push('*');
            i += 2;
            in_block_comment = true;
            continue;
        }

        if b == b'"' {
            out.push('"');
            i += 1;
            in_string = true;
            continue;
        }

        let is_ident_boundary = i == 0 || !is_ident_continue(bytes[i - 1] as char);
        if is_ident_boundary
            && i + CONSOLE_DOT_LOG.len() <= bytes.len()
            && &bytes[i..i + CONSOLE_DOT_LOG.len()] == CONSOLE_DOT_LOG
        {
            let mut j = i + CONSOLE_DOT_LOG.len();
            while j < bytes.len()
                && bytes[j].is_ascii_whitespace()
                && bytes[j] != b'\n'
                && bytes[j] != b'\r'
            {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'(' {
                out.push_str(STDLIB_PRINT_NAME);
                i += CONSOLE_DOT_LOG.len();
                continue;
            }
        }

        out.push(b as char);
        i += 1;
    }

    out
}

fn rewrite_js_arrow_line(line: &str, line_no: usize) -> Result<String, ParseError> {
    let Some(arrow_index) = line.find("=>") else {
        return Ok(line.to_string());
    };

    let left = &line[..arrow_index];
    let right = line[arrow_index + 2..].trim_start();
    if right.starts_with('{') {
        return Err(ParseError {
            line: line_no,
            message: "arrow closures with block bodies are not supported in this subset"
                .to_string(),
        });
    }

    let left_trimmed = left.trim_end();
    let (prefix, params_text) = if left_trimmed.ends_with(')') {
        let mut depth = 0usize;
        let mut open_index = None;
        for (idx, ch) in left_trimmed.char_indices().rev() {
            match ch {
                ')' => depth += 1,
                '(' => {
                    if depth == 0 {
                        return Err(ParseError {
                            line: line_no,
                            message: "malformed arrow closure parameters".to_string(),
                        });
                    }
                    depth -= 1;
                    if depth == 0 {
                        open_index = Some(idx);
                        break;
                    }
                }
                _ => {}
            }
        }
        let open = open_index.ok_or(ParseError {
            line: line_no,
            message: "could not find '(' for arrow closure parameters".to_string(),
        })?;
        (
            &left_trimmed[..open],
            &left_trimmed[open + 1..left_trimmed.len() - 1],
        )
    } else {
        let mut split_index = 0usize;
        for (idx, ch) in left_trimmed.char_indices().rev() {
            if !(ch.is_ascii_alphanumeric() || ch == '_') {
                split_index = idx + ch.len_utf8();
                break;
            }
        }
        (&left_trimmed[..split_index], &left_trimmed[split_index..])
    };

    let params = params_text.trim();
    if params.is_empty() {
        return Err(ParseError {
            line: line_no,
            message: "arrow closure parameters cannot be empty".to_string(),
        });
    }

    Ok(format!("{}|{}| {}", prefix, params, right))
}
