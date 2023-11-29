use std::{collections::HashMap, iter::Peekable, str::Chars};

use crate::{model::Model, typecheck, Basic, FieldName, OclBool, OclLiteral, OclNode, OclType};

// TODO: Cow
type Token = String;

struct StringIter<T: Iterator> {
    iter: Peekable<T>,
    peeked: Option<Token>,
}

fn parse_token(i: &mut Peekable<impl Iterator<Item = char>>) -> Option<Token> {
    let mut c = i.next()?;

    while c.is_whitespace() {
        c = i.next()?;
    }

    let mut tok = String::new();
    tok.push(c);

    if c.is_alphabetic() || c == '_' {
        while i
            .peek()
            .cloned()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
        {
            match i.next() {
                Some(c) => tok.push(c),
                None => break,
            }
        }
    } else if c.is_numeric() {
        while i.peek().cloned().is_some_and(char::is_numeric) {
            match i.next() {
                Some(c) => tok.push(c),
                None => break,
            }
        }
    } else {
        match c {
            ':' => {
                let next = i.peek();
                if let Some(':') = next {
                    i.next();
                    tok.push(':');
                }
            }
            '.' | '(' | '|' | ')' | '=' | '>' | '+' => (),
            '-' | '<' => {
                // -> and <>
                let next = i.peek();
                if let Some('>') = next {
                    i.next();
                    tok.push('>');
                }
            }
            _ => panic!("Invalid char {c}"),
        }
    }

    Some(tok)
}

impl<T: Iterator<Item = char>> StringIter<T> {
    fn from_str<'a>(s: &'a str) -> StringIter<impl Iterator<Item = char> + 'a> {
        StringIter {
            iter: s.chars().peekable(),
            peeked: None,
        }
    }

    fn peek_next_token(&mut self) -> Option<Token> {
        if self.peeked.is_none() {
            self.peeked = parse_token(&mut self.iter);
        }

        self.peeked.clone()
    }

    fn optional_next_token(&mut self) -> Option<Token> {
        match self.peeked.take() {
            Some(t) => Some(t),
            None => parse_token(&mut self.iter),
        }
    }

    fn next_token(&mut self) -> Token {
        self.optional_next_token().expect("Unexpected EOF")
    }
}

#[derive(Default)]
struct VariableTypes {
    parameters: HashMap<String, OclType>,
    iterator_vars: Vec<HashMap<String, OclType>>,
    // TODO: Implicit iterator_vars
}

enum Resolution {
    IterVar(OclType),
    ContextVar(OclType),
    NotFound,
}

impl VariableTypes {
    fn resolve(&self, name: &str) -> Resolution {
        for iter in self.iterator_vars.iter().rev() {
            if let Some(t) = iter.get(name) {
                return Resolution::IterVar(t.clone());
            }
        }

        if let Some(c) = self.parameters.get(name) {
            Resolution::ContextVar(c.clone())
        } else {
            Resolution::NotFound
        }
    }

    fn push_iter(&mut self, hm: HashMap<String, OclType>) {
        self.iterator_vars.push(hm);
    }

    fn pop_iter(&mut self) -> Option<HashMap<String, OclType>> {
        self.iterator_vars.pop()
    }
}

fn parse_iter_var_list(
    text: &mut StringIter<impl Iterator<Item = char>>,
    collection_type: &OclType,
) -> HashMap<String, OclType> {
    let mut map = HashMap::new();
    loop {
        let name = text.next_token();
        if name == "|" {
            break;
        }

        let colon = text.peek_next_token().expect("Unexpected EOF");
        let typ = if colon == ":" {
            parse_type(text.next_token().as_str())
        } else {
            collection_type.clone()
        };

        map.insert(name, typ);

        if text.peek_next_token().expect("Unexpected EOF") == "," {
            drop(text.next_token())
        }
    }
    map
}

fn parse_type(text: &str, /* , model: &Model TODO: validate that the class exists */) -> OclType {
    match text {
        "String" => OclType::Basic(Basic::String),
        "Boolean" => OclType::Basic(Basic::Boolean),
        "Real" => OclType::Basic(Basic::Real),
        "Integer" => OclType::Basic(Basic::Integer),
        x => OclType::Class(x.into()),
    }
}

type Parameters = HashMap<String, OclType>;

pub(crate) fn parse_full_query<'a>(text: &'a str, model: &Model) -> (OclNode, Parameters) {
    type T<'a> = Chars<'a>;
    let mut text = StringIter::<T>::from_str(text);
    let mut ctx = VariableTypes::default();

    let tok = text.peek_next_token().expect("Empty query");

    if tok.starts_with("param") {
        drop(text.next_token());
        loop {
            let name = text.next_token();
            assert_eq!(text.next_token(), ":");
            if name == "in" {
                break;
            }

            let typ = text.next_token();

            ctx.parameters.insert(name, parse_type(typ.as_str()));

            if text
                .peek_next_token()
                .expect("Unexpected EOF inside parameter declaration")
                == ","
            {
                drop(text.next_token())
            }
        }
    }

    (
        parse_expr(&mut text, None, &mut ctx, model),
        ctx.parameters.clone(),
    )
}

fn parse_expr(
    text: &mut StringIter<impl Iterator<Item = char>>,
    stop_token: Option<Token>,
    ctx: &mut VariableTypes,
    model: &Model,
) -> OclNode {
    let mut cur_node = parse_base_case(text, ctx, model);

    loop {
        let next = text.optional_next_token();
        if next == stop_token {
            break;
        }
        let next = next.expect("Unexpected EOF in expression");
        match next.as_str() {
            "." => {
                let typ = typecheck(&cur_node, model);
                let field_name = FieldName::from_string(text.next_token());
                match typ {
                    OclType::Basic(_) => todo!("Can primitive objects have navigations?"),
                    OclType::Class(name) => {
                        // TODO: Add a resolve method to Class
                        let class = &model.classes[&name];
                        let field_type = model.field_of(class, &field_name).unwrap_or_else(|| {
                            panic!("No navigation {field_name:?} for class {class:?}")
                        });
                        match field_type {
                            OclType::Basic(_) => {
                                cur_node = OclNode::BasicAttribute(Box::new(cur_node), field_name)
                            }
                            OclType::Class(_) => {
                                cur_node = OclNode::Navigate(Box::new(cur_node), field_name)
                            }
                        }
                    }
                }
            }
            "->" => {
                let op = text.next_token();
                match op.as_str() {
                    "select" => {
                        assert_eq!(text.next_token(), "(");
                        // TODO: Peek ahead. If there is no var list, declare an unnameable implicit iter var
                        ctx.push_iter(parse_iter_var_list(text, &typecheck(&cur_node, model)));
                        // FIXME: Non statically known bool exprs can maybe exist? e.g. object.bool_field
                        let select_clause = parse_expr(text, Some(")".into()), ctx, model);
                        assert!(matches!(
                            typecheck(&select_clause, model),
                            OclType::Basic(Basic::Boolean)
                        ));
                        let vars = ctx.pop_iter();
                        cur_node = OclNode::Select(
                            vars.unwrap().clone(),
                            Box::new(cur_node),
                            Box::new(select_clause),
                        );
                    }
                    "size" => {
                        assert_eq!(text.next_token(), "(");
                        assert_eq!(text.next_token(), ")");
                        cur_node = OclNode::Count(Box::new(cur_node));
                    }
                    "first" => {
                        assert_eq!(text.next_token(), "(");
                        assert_eq!(text.next_token(), ")");
                        cur_node = OclNode::First(Box::new(cur_node));
                    }
                    _ => panic!("Unrecognized collection operation"),
                }
            }
            "-" | "+" | "=" | "<>" | "<" | ">" => {
                let bin = next;
                let rhs = parse_expr(text, stop_token.clone(), ctx, model);
                cur_node = match bin.as_str() {
                    ">" => OclNode::Bool(Box::new(OclBool::GreaterThan(cur_node, rhs))),
                    "<" => OclNode::Bool(Box::new(OclBool::LessThan(cur_node, rhs))),
                    "<>" => OclNode::Bool(Box::new(OclBool::NotEquals(cur_node, rhs))),
                    x => todo!("Unknown binop {:?}", x),
                };
                break;
                //todo!("Re-jig the tree for operator precedence?");
            }

            _ => panic!("Invalid syntax"),
        }
    }

    cur_node
}

fn is_numeric(s: &str) -> bool {
    s.chars().all(char::is_numeric)
}

fn parse_base_case(
    text: &mut StringIter<impl Iterator<Item = char>>,
    ctx: &mut VariableTypes,
    model: &Model,
) -> OclNode {
    let name = text.next_token();
    // check to see if it's a literal? For security reasons could disallow string literals to not allow people to make OCL injections
    if name == "(" {
        return parse_expr(text, Some(")".to_string()), ctx, model);
    } else if is_numeric(&name) {
        let next = text.peek_next_token();
        if let Some(".") = next.as_deref() {
            let mut float = name;
            float.push_str(&text.next_token());
            float.push_str(&text.next_token());
            let float: f64 = float.parse().expect("Invalid float literal");
            OclNode::Literal(OclLiteral::Real(float))
        } else {
            let integer: i64 = name.parse().unwrap();
            OclNode::Literal(OclLiteral::Integer(integer))
        }
    } else if name == "true" {
        return OclNode::Literal(OclLiteral::Boolean(true));
    } else if name == "false" {
        return OclNode::Literal(OclLiteral::Boolean(false));
    } else {
        match ctx.resolve(&name) {
            Resolution::IterVar(c) => OclNode::IterVariable(name, c),
            Resolution::ContextVar(c) => OclNode::ContextVariable(name, c),
            Resolution::NotFound => {
                let class_name = name.as_str().into();
                if model.classes.contains_key(&class_name) {
                    assert_eq!(text.next_token(), ".");
                    assert_eq!(text.next_token(), "allInstances");
                    return OclNode::AllInstances(class_name);
                } else if let Some(e) = model.enums.get(&name) {
                    assert_eq!(text.next_token(), "::");

                    let member = text.next_token();

                    assert!(
                        e.index_of(&member).is_some(),
                        "Invalid member {member} for enum {name}"
                    );
                    return OclNode::EnumMember(name, member);
                } else {
                    // TOOD: Add an implicit iter variable here
                    // The spec is a little tricky here.
                    // Find the implicit iter variable that matches the next navigation.
                    // If it's ambiguous then throw an error
                    panic!("Unknown name {name:?}")
                }
            }
        }
    }
}
