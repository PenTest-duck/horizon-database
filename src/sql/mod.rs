//! SQL front-end for Horizon DB.
//!
//! This module contains the lexer (tokenizer), abstract syntax tree (AST)
//! definitions, and a recursive-descent parser that transforms raw SQL text
//! into a structured AST suitable for query planning and execution.

pub mod lexer;
pub mod ast;
pub mod parser;

pub use ast::*;
pub use lexer::Token;
