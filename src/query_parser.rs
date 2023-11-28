use std::{collections::HashMap, iter::Peekable, str::Chars};

use crate::{
    model::Model, typecheck, ClassName, FieldName, OclBool, OclNode, OclType, Primitive,
    SqlIdentifier,
};

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

    if c.is_alphabetic() {
        while i.peek().cloned().is_some_and(char::is_alphanumeric) {
            match i.next() {
                Some(c) => tok.push(c),
                None => break,
            }
        }
    } else {
        match c {
            ':' | '.' | '(' | '|' | ')' | '=' | '>' | '+' => (),
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
struct ParseContext {
    parameters: HashMap<String, OclType>,
    iterator_vars: Vec<HashMap<String, OclType>>,
}

enum Resolution {
    IterVar(OclType),
    ContextVar(OclType),
    NotFound,
}

impl Resolution {
    fn unwrap_iter(self) -> OclType {
        match self {
            Resolution::IterVar(c) => c,
            Resolution::ContextVar(_) => {
                panic!("Iter variable not found but context var with same name was found")
            }
            Resolution::NotFound => panic!("Iter variable not found"),
        }
    }

    fn unwrap_context(self) -> OclType {
        match self {
            Resolution::ContextVar(c) => c,
            Resolution::IterVar(_) => {
                panic!("Context variable not found but iter var with same name was found")
            }
            Resolution::NotFound => panic!("Context variable not found"),
        }
    }
}

impl ParseContext {
    fn with_parameters(params: HashMap<String, OclType>) -> Self {
        ParseContext {
            parameters: params,
            ..ParseContext::default()
        }
    }

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

    fn resolve_mut(&mut self, name: &str) -> Option<&mut OclType> {
        for iter in self.iterator_vars.iter_mut().rev() {
            if let Some(t) = iter.get_mut(name) {
                return Some(t);
            }
        }

        if let Some(c) = self.parameters.get_mut(name) {
            Some(c)
        } else {
            None
        }
    }

    fn push_iter(&mut self, hm: HashMap<String, OclType>) {
        self.iterator_vars.push(hm);
    }

    fn pop_iter(&mut self) {
        self.iterator_vars.pop();
    }
}

fn parse_iter_var_list(
    text: &mut StringIter<impl Iterator<Item = char>>,
) -> HashMap<String, OclType> {
    let mut map = HashMap::new();
    while true {
        let name = text.next_token();
        if name == "|" {
            break;
        }

        assert_eq!(text.next_token(), ":");

        let typ = parse_type(text.next_token().as_str());

        map.insert(name, typ);

        if text.peek_next_token().expect("Unexpected EOF") == "," {
            drop(text.next_token())
        }
    }
    map
}

fn parse_type(text: &str, /* , model: &Model TODO: validate that the class exists */) -> OclType {
    match text {
        "String" => OclType::Primitive(Primitive::String),
        "Boolean" => OclType::Primitive(Primitive::Boolean),
        "Real" => OclType::Primitive(Primitive::Real),
        "Integer" => OclType::Primitive(Primitive::Integer),
        x => OclType::Class(ClassName(x.to_string())),
    }
}

type Parameters = HashMap<String, OclType>;

pub(crate) fn parse_full_query<'a>(text: &'a str, model: &Model) -> (OclNode, Parameters) {
    type T<'a> = Chars<'a>;
    let mut text = StringIter::<T>::from_str(text);
    let mut ctx = ParseContext::default();

    let mut tok = text.peek_next_token().expect("Empty query");

    if tok == "bikeshed" {
        drop(text.next_token());
        while true {
            let name = text.next_token();
            assert_eq!(text.next_token(), ":");
            if name == "in" {
                break;
            }

            let typ = text.next_token();

            ctx.parameters.insert(name, (parse_type(typ.as_str())));

            if text
                .peek_next_token()
                .expect("Unexpected EOF inside bikeshed declaration")
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
    ctx: &mut ParseContext,
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
                let field_name = FieldName(text.next_token());
                match typ {
                    OclType::Primitive(_) => todo!("Can primitive objects have navigations?"),
                    OclType::Class(name) => {
                        // TODO: Add a resolve method to Class
                        let class = &model.classes[&name];
                        let field_type = model.field_of(class, &field_name).unwrap_or_else(|| {
                            panic!("No navigation {field_name:?} for class {class:?}")
                        });
                        match field_type {
                            OclType::Primitive(_) => {
                                cur_node = OclNode::PrimitiveField(Box::new(cur_node), field_name)
                            }
                            OclType::Class(_) => {
                                cur_node = OclNode::Navigate(Box::new(cur_node), field_name)
                            }
                        }
                    }
                }
            }
            "->" => match text.next_token().as_str() {
                "select" => {
                    assert_eq!(text.next_token(), "(");
                    ctx.iterator_vars.push(parse_iter_var_list(text));
                    // FIXME: Non statically known bool exprs can maybe exist? e.g. object.bool_field
                    let select_clause = parse_expr(text, Some(")".into()), ctx, model);
                    assert!(matches!(
                        typecheck(&select_clause, model),
                        OclType::Primitive(Primitive::Boolean)
                    ));
                    let vars = ctx.iterator_vars.pop();
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
                _ => panic!("Unrecognized collection operation"),
            },
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

fn parse_base_case(
    text: &mut StringIter<impl Iterator<Item = char>>,
    ctx: &mut ParseContext,
    model: &Model,
) -> OclNode {
    let name = text.next_token();
    // check to see if it's a literal? For security reasons could disallow string literals to not allow people to make OCL injections
    if name == "(" {
        return parse_expr(text, Some(")".to_string()), ctx, model);
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
                    return OclNode::EnumMember(name, text.next_token());
                } else {
                    panic!("Unknown name {name:?}")
                }
            }
        }
    }
}
