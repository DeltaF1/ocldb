use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Read, Write};

mod model;

mod name {
    use std::marker::PhantomData;

    #[derive(Debug, Hash, Eq, PartialEq, Clone, PartialOrd, Ord)]
    pub struct Name<T>(String, PhantomData<T>);

    pub trait NameType {}
    pub trait SqlNameType {}

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
    pub struct ClassName;
    impl NameType for ClassName {}

    #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
    pub struct FieldName;
    impl NameType for FieldName {}

    #[derive(Clone, Copy)]
    pub struct TableAlias;
    impl NameType for TableAlias {}
    impl SqlNameType for TableAlias {}

    #[derive(Clone, Copy, Hash, Eq, PartialEq)]
    pub struct TableName;
    impl NameType for TableName {}
    impl SqlNameType for TableName {}

    #[derive(Clone, Copy)]
    pub struct ColumnName;
    impl NameType for ColumnName {}
    impl SqlNameType for ColumnName {}

    impl<T> From<&str> for Name<T> {
        fn from(s: &str) -> Self {
            Name(s.to_string(), PhantomData)
        }
    }

    impl<T> From<String> for Name<T> {
        fn from(value: String) -> Self {
            Name(value, PhantomData)
        }
    }

    impl<T: NameType> Name<T> {
        pub(crate) fn from_string(s: String) -> Name<T> {
            Name(s, PhantomData)
        }
        pub(crate) fn into_inner(self) -> String {
            self.0
        }

        pub fn as_str(&self) -> &str {
            &self.0
        }
    }

    impl<T: SqlNameType> Name<T> {
        pub fn escape(&self) -> String {
            format!("\"{}\"", self.0.replace("\"", "\"\""))
        }
    }
}

type ClassName = name::Name<name::ClassName>;
type FieldName = name::Name<name::FieldName>;

#[derive(Debug, Clone, PartialEq)]
pub enum Basic {
    Real,
    Integer,
    String,
    Boolean,
    Enum(String), // TODO Enum(...)
}

#[derive(Clone, Debug)]
enum OclLiteral {
    Real(f64),
    Integer(i64),

    // FIXME: String literals should not be allowed to discourage OCL injection?
    #[deprecated]
    String(String),
    Boolean(bool),
}

// TODO: Mark whether the type is single or a collection
/*
enum Scalar {
    Primitive(Primitive),
    Class(ClassName),
    Enum(EnumName)
}

enum OclType {
    Bag(Scalar),
    Set(Scalar),
    Sequence(Scalar),
    Scalar(Scalar)
}
 */
#[derive(Clone, Debug, PartialEq)]
pub enum OclType {
    Basic(Basic),
    Class(ClassName),
}

impl OclType {
    fn class_name(&self) -> &ClassName {
        match self {
            OclType::Basic(_) => panic!("No class name for primitive"),
            OclType::Class(name) => name,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Associativity {
    One,
    Optional,
    Many,
}

/*
Create object

Lookup class
for all fields: A value must be specified
For all outgoing relationships: If assoc == One, then specify another object.
If the backwards assoc is also One, then use the current object.
Repeat for all superclasses

// allow external variables in create as well
let some_doctor: Doctor in
create Patient {
    age: 20,
    name: "Alice",
    kit: create Kit { // 1-1 relationship will automatically link the new Kit back to this Patient object
        meal_allowance: 3.50
    },
    hospital: query {
        some_doctor.hospital
    },
    emergency_contact: query {
        some_doctor.surgeries->select(s: Surgery | s.outcome = Good).lead_doctor->first()
    }
}

create Hospital {
    doctors: Set {
        create Doctor {

        }
    }
}

INSERT INTO Person_class (Person_age, Person_name) VALUES (?=20, ?='alice');
let last_id = SELECT last_id();

let hospital = SELECT Doctor_class0.hospital FROM Doctor_class as Doctor_class0 WHERE Doctor_class0.id = ?=some_doctor;
let contact = SELECT Person_class0.id FROM
Doctor_class as Doctor_class0

INSERT INTO Patient_class (id, Patient_hospital, Patient_emergency_contact, Kit_meal_allowance)
VALUES (?=last_id, ?=hospital, )

create Doctor {
    age: 44,
    name: "Dave"
} link hospital -> create Hospital {
    name: "Mt. Sinai"
}

OR

create Doctor {
    age: 44,
    name: "Dave"
} link hospital -> query {
    Hospital.allInstances->select(h: Hospital | h.doctors->size() > 10)->first()
}

let some_doctor: Doctor
let some_other_doctor: Doctor
in
create Surgery {
    outcome: Bad,
    head_surgeon: some_doctor // Special case for context vars. This is equivalent to `lead_doctor: query { some_doctor }`
    normal_doctors: query {
        // This surgery was attended by all other doctors at this hospital and one other doctor maybe not at this hospital
        some_doctor.hospital.doctors->excluding(some_doctor)->including(some_other_doctor)
    }
}

struct Object {
    id: usize,
    type: ClassName
}

ctx = {("some_doctor", "Doctor")}
model.create_object("Patient")
    .set_field("age", 20)
    .set_field("name", "Alice")
    .set_relationship("kit",
        model.create_object("Kit")
            .set_field("mess_allowance", 3.50)
            // Kit and Patient are in the same table! Need a special case to UPDATE instead of INSERT
    )
    .set_relationship("hospital", query(model, "some_doctor.hospital"))
    .set_relationship("emergency_contacts", query(model, "some_doctor.surgeries->select(s: Surgery | s.outcome = Good).lead_doctor->first()"))

->

let emergency_contact = SELECT Surgery0.lead_doctor FROM Doctor0 JOIN manymanySurgeries ON manymanySurgeries.doctor = Doctor0.id JOIN Surgery0 ON manmanysurgeries.surgery = Surgery0.id WHERE Doctor0.id = ?self LIMIT 1;
let hospital = SELECT Doctor0.hospital FROM Doctor0 WHERE Doctor0.id = ?self;
// Can't create a Kit without creating a Person first
let person = INSERT INTO Person_class(Person_age, Person_name) VALUES (20, "Alice");
let kit = INSERT INTO Kit_class(id, Kit_mess_allowance) VALUES (?); ?=person, ?=3.50
let patient = UPDATE Kit_class SET Patient_hospital = ?, Patient_emergency_contact) = ? WHERE id=?; ?=hospital, ?=emergency_contact, ?=kit

 */

/* Update object

   // TODO: Destroying or updating a 1-1 link will need to destroy the other object
   //   that might be taken care of by foreign keys

let self: Doctor in
update self {
   ???
}

 */

type TableAlias = name::Name<name::TableAlias>;
type TableName = name::Name<name::TableName>;
type ColumnName = name::Name<name::ColumnName>;

mod sql_tree {
    enum Constant {
        Real(f64),
        Integer(i64),
        String(String),
    }

    impl Display for Constant {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Constant::Real(_) => todo!(),
                Constant::Integer(i) => write!(f, "{i}"),
                Constant::String(s) => write!(f, "'{s}'"), //FIXME: Prevent SQL injection
            }
        }
    }

    impl From<OclLiteral> for Constant {
        fn from(value: OclLiteral) -> Self {
            match value {
                OclLiteral::Real(f) => Constant::Real(f),
                OclLiteral::Integer(i) => Constant::Integer(i),
                OclLiteral::String(s) => Constant::String(s),
                OclLiteral::Boolean(b) => Constant::Integer(b as i64),
            }
        }
    }

    impl From<f64> for Constant {
        fn from(value: f64) -> Self {
            Constant::Real(value)
        }
    }

    impl From<i64> for Constant {
        fn from(value: i64) -> Self {
            todo!()
        }
    }

    impl From<String> for Constant {
        fn from(value: String) -> Self {
            todo!()
        }
    }

    impl From<&str> for Constant {
        fn from(value: &str) -> Self {
            Constant::String(value.to_string())
        }
    }

    enum ColumnSpec {
        Star,
        Named(ColumnName),
    }

    impl ColumnSpec {
        fn named<S: Into<String>>(s: S) -> ColumnSpec {
            ColumnSpec::Named(s.into().into())
        }
    }

    impl std::fmt::Display for ColumnSpec {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ColumnSpec::Star => write!(f, "*"),
                ColumnSpec::Named(col) => write!(f, "{}", col.escape()),
            }
        }
    }

    enum Expr {
        Field(TableAlias, ColumnSpec),
        Constant(Constant),
        Parameter(String),
    }

    impl std::fmt::Display for Expr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            match self {
                Expr::Field(table, column) => write!(f, "{}.{}", table.escape(), column),
                Expr::Constant(c) => write!(f, "{c}"),
                Expr::Parameter(name) => write!(f, "${}", name),
            }
        }
    }

    enum BoolCondition {
        And(Box<BoolCondition>, Box<BoolCondition>),
        Equal(Expr, Expr),
        NotEqual(Expr, Expr),
        GreaterThan(Expr, Expr),
        LessThan(Expr, Expr),
    }

    impl std::fmt::Display for BoolCondition {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            match self {
                BoolCondition::And(a, b) => write!(f, "{a} AND\n {b}"),
                BoolCondition::Equal(a, b) => write!(f, "{a} = {b}"),
                BoolCondition::NotEqual(a, b) => write!(f, "{a} != {b}"),
                BoolCondition::GreaterThan(a, b) => write!(f, "{a} > {b}"),
                BoolCondition::LessThan(a, b) => write!(f, "{a} < {b}"),
            }
        }
    }

    use std::{collections::HashMap, fmt::Display, num::NonZeroUsize};

    use crate::{
        model::{self, canonicalize},
        typecheck, Associativity, Basic, ClassName, ColumnName, OclBool, OclContext, OclLiteral,
        OclNode, OclType, Resolution, TableAlias, TableName,
    };

    enum Selectable {
        Field(TableAlias, ColumnSpec),
        Constant(Constant),
        Parameter(String),
        Star,
    }

    impl Display for Selectable {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Selectable::Field(tbl, field) => write!(f, "{}.{field}", tbl.escape()),
                Selectable::Constant(c) => write!(f, "{c}"),
                Selectable::Parameter(p) => write!(f, "${p}"),
                Selectable::Star => write!(f, "*"),
            }
        }
    }

    // FIXME this should include a field descriptor too
    enum Output {
        Select(Selectable),
        Count(Selectable),
    }

    impl Display for Output {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Output::Select(e) => write!(f, "{}", e),
                Output::Count(e) => write!(f, "COUNT({})", e),
            }
        }
    }

    impl InProgressQuery {
        fn to_count(self, s: Selectable) -> Query {
            Query {
                output: Output::Count(s),
                body: Some(self.body.unwrap()),
                r#where: self.r#where,
                limit: None,
            }
        }

        fn to_select_table(self, table: TableAlias, column: ColumnSpec) -> Query {
            self.to_select(Selectable::Field(table, column))
        }

        fn to_select(self, e: Selectable) -> Query {
            if matches!(e, Selectable::Field(_, _)) {
                assert!(self.body.is_some());
            }
            Query {
                output: Output::Select(e),
                body: self.body,
                r#where: self.r#where,
                limit: None,
            }
        }

        fn to_limited(
            self,
            table: TableAlias,
            columnspec: ColumnSpec,
            limit: NonZeroUsize,
        ) -> Query {
            Query {
                output: Output::Select(Selectable::Field(table, columnspec)),
                body: Some(self.body.unwrap()),
                r#where: self.r#where,
                limit: Some(limit),
            }
        }
    }

    #[derive(Default)]
    struct Aliases {
        tables: HashMap<TableName, usize>,
    }

    use std::fmt::Write;
    impl Aliases {
        fn new_alias(&mut self, name: TableName) -> TableAlias {
            let n = self.tables.entry(name.clone()).or_default();
            *n += 1;
            let mut name = name.into_inner();
            write!(name, "_{}", n).unwrap();
            TableAlias::from(name)
        }
    }

    #[derive(Default)]
    struct InProgressQuery {
        body: Option<Body>,
        r#where: Option<BoolCondition>,
        limit: Option<NonZeroUsize>,
    }

    impl InProgressQuery {
        fn add_base_table(&mut self, name: TableName, aliases: &mut Aliases) -> TableAlias {
            assert!(self.body.is_none());

            let alias = aliases.new_alias(name.clone());

            self.body = Some(Body::Named(Table::Table(name), alias.clone()));
            alias
        }

        fn add_joined_table(
            &mut self,
            from: (TableAlias, ColumnName),
            to: (TableName, ColumnName),
            aliases: &mut Aliases,
        ) -> TableAlias {
            let new_alias = aliases.new_alias(to.0.clone());
            let condition = BoolCondition::Equal(
                Expr::Field(from.0, ColumnSpec::Named(from.1)),
                Expr::Field(new_alias.clone(), ColumnSpec::Named(to.1)),
            );

            match self.body.take() {
                Some(body) => {
                    let b = Body::Join(
                        Box::new(body),
                        Box::new(Body::Named(Table::Table(to.0), new_alias.clone())),
                        condition,
                    );
                    self.body = Some(b);
                }
                None => panic!("No base table"),
            }
            new_alias
        }

        fn add_where(&mut self, cond: BoolCondition) {
            self.r#where = match self.r#where.take() {
                Some(existing_cond) => {
                    Some(BoolCondition::And(Box::new(existing_cond), Box::new(cond)))
                }
                None => Some(cond),
            }
        }
    }

    pub struct Query {
        output: Output,
        body: Option<Body>,
        r#where: Option<BoolCondition>,
        limit: Option<NonZeroUsize>,
    }

    impl Display for Query {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SELECT {}", self.output)?;

            if let Some(body) = &self.body {
                write!(f, "\n FROM {}", body)?;
            }

            if let Some(w) = &self.r#where {
                write!(f, "\n WHERE {w}")?;
            }
            if let Some(u) = self.limit {
                write!(f, "\n LIMIT {u}")?;
            }
            write!(f, ";")
        }
    }

    enum Body {
        // TODO: Need to have LeftJoin for when the right could be an empty set
        Join(Box<Body>, Box<Body>, BoolCondition),
        Named(Table, TableAlias),
    }

    impl Body {
        fn last_alias(&self) -> TableAlias {
            match self {
                Body::Join(_, next, _) => next.last_alias(),
                Body::Named(_, alias) => alias.clone(),
            }
        }
    }

    impl Display for Body {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Body::Join(base, next, condition) => {
                    write!(f, "{} JOIN {} ON {}\n", base, next, condition)
                }
                Body::Named(table, alias) => write!(f, "{} AS {}", table, alias.escape()),
            }
        }
    }

    enum Table {
        Query(Box<Query>),
        Table(TableName),
    }

    impl Display for Table {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Table::Query(q) => write!(f, "(\n{}\n)", q),
                Table::Table(name) => write!(f, "{}", name.escape()),
            }
        }
    }

    fn is_base(node: &OclNode) -> bool {
        match node {
            OclNode::IterVariable(_, _) => true,
            OclNode::ContextVariable(_, _) => true,
            OclNode::AllInstances(_) => true,
            OclNode::Navigate(_, _) => false,
            OclNode::BasicAttribute(_, _) => false,
            OclNode::Select(_, _, _) => false,
            OclNode::First(_) => false,
            OclNode::Count(_) => false,
            OclNode::Bool(_) => todo!(),
            OclNode::EnumMember(_, _) => true,
            OclNode::Literal(_) => true,
        }
    }

    #[derive(Default)]
    struct QueryState {
        aliases: HashMap<String, usize>,
        bindings: HashMap<String, TableAlias>,
        upcasts: HashMap<(TableAlias, ClassName), TableAlias>, // Keep track of the alias of the upcast table for the given table/type
    }

    pub(crate) fn ocl_to_sql(
        ocl: &OclNode,
        parameters: &HashMap<String, OclType>,
        model: &crate::model::Model,
    ) -> Query {
        // TODO: Return a Query and a ParameterInfo struct to support non sqlite parameters
        let mut aliases = Aliases::default();
        let mut context = OclContext::default();
        let mut state = QueryState::default();
        // match the top-level node to determine output
        match ocl {
            OclNode::IterVariable(_, _) => panic!("Invalid OCL query"),
            OclNode::ContextVariable(varname, typ) => match typ {
                OclType::Class(_) => todo!("build_query"),
                OclType::Basic(p) => Query {
                    output: Output::Select(Selectable::Parameter(varname.to_string())),
                    body: None,
                    r#where: None,
                    limit: None,
                },
            },
            OclNode::AllInstances(class) => Query {
                output: Output::Select(Selectable::Star),
                body: Some(Body::Named(
                    Table::Table(model.table_of(class)),
                    "out".into(),
                )),
                r#where: None,
                limit: None,
            },
            OclNode::Navigate(_, _) => {
                let in_progress = InProgressQuery::default();
                let (in_progress, table) =
                    build_query(ocl, model, in_progress, &mut context, &mut aliases);

                in_progress.to_select_table(table, ColumnSpec::Star)
            }
            OclNode::BasicAttribute(node, field) => {
                let in_progress = InProgressQuery::default();
                let (in_progress, table) =
                    build_query(ocl, model, in_progress, &mut context, &mut aliases);
                let OclType::Class(class) = typecheck(node, model) else {
                    panic!("Tried to get primitive field of non-class object")
                };
                let columnspec = canonicalize::column_name(class, field);
                in_progress.to_select_table(table, ColumnSpec::Named(columnspec))
            }
            OclNode::Select(_, _, _) => {
                let in_progress = InProgressQuery::default();
                let (in_progress, table) =
                    build_query(ocl, model, in_progress, &mut context, &mut aliases);

                in_progress.to_select_table(table, ColumnSpec::Star)
            }
            OclNode::First(node) => {
                let in_progress = InProgressQuery::default();
                let (in_progress, table) =
                    build_query(node, model, in_progress, &mut context, &mut aliases);
                in_progress.to_limited(table, ColumnSpec::Star, NonZeroUsize::new(1).unwrap())
            }
            OclNode::Count(node) => {
                let in_progress = InProgressQuery::default();
                let (in_progress, table) =
                    build_query(node, model, in_progress, &mut context, &mut aliases);

                in_progress.to_count(Selectable::Field(table, ColumnSpec::Star))
            }
            OclNode::Bool(b) => {
                let in_progress: InProgressQuery = todo!("parse_expr_part");
                in_progress.to_count(Selectable::Star)
            }
            OclNode::Literal(literal) => Query {
                output: Output::Select(Selectable::Constant(literal.clone().into())),
                body: None,
                r#where: None,
                limit: None,
            },
            OclNode::EnumMember(name, member) => {
                let Some(constant) = model.enums[name].index_of(member) else {
                    panic!("Invalid field {member:?} for enum {name}")
                };
                Query {
                    output: Output::Select(Selectable::Constant(Constant::Integer(
                        constant as i64,
                    ))),
                    body: None,
                    r#where: None,
                    limit: None,
                }
            }
        }
    }

    fn build_query(
        ocl: &OclNode,
        model: &crate::model::Model,
        mut sql: InProgressQuery,
        context: &mut OclContext,
        aliases: &mut Aliases,
    ) -> (InProgressQuery, TableAlias) {
        match ocl {
            OclNode::IterVariable(name, _) => {
                let var = context.resolve(name);
                (sql, var.unwrap_iter())

                // if the variable is an upcast, add a join onto the base class
            }
            OclNode::ContextVariable(name, typ) => {
                let alias = match context.resolve(name) {
                    Resolution::IterVar(_) => unreachable!(),
                    Resolution::Parameter(alias) => alias,
                    Resolution::NotFound => {
                        let tbl = match typ {
                            OclType::Class(ident) => ident,
                            _ => todo!("Non-object variables"),
                        };

                        let alias = sql.add_base_table(
                            crate::model::canonicalize::class_table_name(&tbl),
                            aliases,
                        );

                        sql.add_where(BoolCondition::Equal(
                            Expr::Field(alias.clone(), ColumnSpec::Named("id".into())),
                            Expr::Parameter(name.clone()),
                        ));

                        context.parameters.insert(name.clone(), alias.clone());

                        alias
                    }
                };

                (sql, alias)
            }
            OclNode::AllInstances(tbl) => {
                let alias = sql.add_base_table(model.table_of(tbl), aliases);
                (sql, alias)
            }
            OclNode::Select(iter_vars, node, condition) => {
                // TODO: Collect the types of the Iter variable
                // if the type is not a supertype then error
                // Otherwise the upcasting should Just Work™
                // Downcasting is not allowed
                let (sql, top_of_stack_alias) = build_query(node, model, sql, context, aliases);

                // TODO: Break out this binding process into its own method

                if iter_vars.len() > 1 {
                    todo!("Bind multiple iter vars")
                }

                let mut vars_with_aliases = HashMap::new();
                let output_typ = typecheck(node, model);
                for (varname, vartype) in iter_vars {
                    if vartype != &output_typ {
                        let output_class = output_typ.class_name();
                        let var_class = vartype.class_name();
                        assert!(
                            model.is_subclass_of(output_class, var_class),
                            "Can't upcast {output_typ:?} into {vartype:?}"
                        );
                    }

                    // TODO: If there are multiple vars, need to add a new copy of the alias
                    let binding = top_of_stack_alias.clone();

                    vars_with_aliases.insert(varname.clone(), binding);
                }

                context.push_iter(vars_with_aliases);

                fn parse_expr_part(
                    node: &OclNode,
                    model: &model::Model,
                    sql: InProgressQuery,
                    context: &mut OclContext,
                    aliases: &mut Aliases,
                ) -> (InProgressQuery, Expr) {
                    let expr;

                    let sql = match node {
                        OclNode::BasicAttribute(node, field) => {
                            let typ: OclType;
                            let mut ret_sql;
                            let alias = if let OclNode::IterVariable(varname, ty) = &**node {
                                let alias = context.resolve(varname).unwrap_iter();
                                typ = ty.clone();
                                ret_sql = sql;
                                alias
                            } else {
                                let alias;
                                (ret_sql, alias) = build_query(node, model, sql, context, aliases);
                                typ = typecheck(node, model);
                                alias
                            };

                            let OclType::Class(class) = typ else { panic!() };

                            // FIXME: Only add this join if no upcast exists for this alias yet
                            let (class_table, column_name) =
                                model.table_of_prim_field(&class, field).unwrap();
                            let alias = ret_sql.add_joined_table(
                                (alias.clone(), "id".into()),
                                (class_table, "id".into()),
                                aliases,
                            );
                            let columnspec = ColumnSpec::Named(column_name);
                            expr = Expr::Field(alias, columnspec);
                            ret_sql
                        }
                        OclNode::Literal(l) => {
                            expr = Expr::Constant(l.clone().into());
                            sql
                        }
                        OclNode::Count(node) => {
                            let (sql, alias) = build_query(node, model, sql, context, aliases);
                            expr = todo!("");
                            sql
                        }
                        x => todo!("{:?}", x),
                    };

                    (sql, expr)
                }

                let ret = if let OclNode::Bool(bool) = &**condition {
                    match &**bool {
                        OclBool::LessThan(node1, node2) => {
                            let left_expr: Expr;
                            let right_expr: Expr;

                            let (lhs, left_expr) =
                                parse_expr_part(node1, model, sql, context, aliases);
                            let (rhs, right_expr) =
                                parse_expr_part(node2, model, lhs, context, aliases);

                            let mut sql = rhs;
                            sql.add_where(BoolCondition::LessThan(left_expr, right_expr));
                            (sql, top_of_stack_alias)
                        }
                        _ => todo!(),
                    }
                } else {
                    assert!(matches!(
                        typecheck(condition, model),
                        OclType::Basic(Basic::Boolean)
                    ));
                    todo!("We need to do some magic joining and then WHERE result.bool_field = 1");
                };
                context.pop_iter();
                ret
            }
            OclNode::First(_) => todo!(),
            OclNode::Count(node) => {
                let (sql, table_name) = build_query(node, model, sql, context, aliases);
                let subquery =
                    sql.to_count(Selectable::Field(table_name.clone(), ColumnSpec::Star));
                let alias = aliases.new_alias(todo!("table name from alias"));
                (
                    InProgressQuery {
                        body: Some(Body::Named(Table::Query(Box::new(subquery)), alias.clone())),
                        r#where: None,
                        limit: None,
                    },
                    alias,
                )
            }
            OclNode::BasicAttribute(node, navigation) => {
                todo!("Return type for queries that produce primitives")
            }
            OclNode::Navigate(node, navigation) => {
                // TODO: peek at node. If it's a Navigate, then we can potentially avoid an
                // intermediate table if the structure goes
                //      1                 1
                // ... ---> intermediate ---> this table
                //
                //       1
                // ... <---> this table

                let (mut sql, prev_alias) = build_query(node, model, sql, context, aliases);
                let prev_type = typecheck(node, model);
                let OclType::Class(prev_type) = prev_type else {
                    panic!("Tried to navigate on a primitive")
                };
                // TODO: check for primitive field navigations

                let assoc = &model.classes[&prev_type].associations[navigation];

                // TODO: If navigating to a class contained within the same SQL table (i.e. navigation is a true 1-1 association), just return prev_alias
                match assoc.associativity {
                    Associativity::One => {
                        let new_type = assoc.target.clone();
                        let new_table = model.table_of(&assoc.target);
                        let field_name = canonicalize::column_name(prev_type, &navigation);
                        let new_alias = sql.add_joined_table(
                            (prev_alias, field_name),
                            (new_table, "id".into()),
                            aliases,
                        );
                        (sql, new_alias)
                    }
                    Associativity::Many => {
                        let rev = assoc.reverse();
                        let new_type = assoc.target.clone();
                        let new_table = model.table_of(&assoc.target);
                        let field_name =
                            canonicalize::column_name(new_type.clone(), &rev.navigation);
                        let new_alias = match rev.associativity {
                            Associativity::One => sql.add_joined_table(
                                (prev_alias, "id".into()),
                                (new_table, field_name),
                                aliases,
                            ),
                            x => todo!("{:?}", x),
                        };
                        (sql, new_alias)
                    }
                    _ => todo!(),
                }

                //add table
                //add join to prev_alias
                //if it's a many-many then add additional target table and join
            }
            OclNode::Bool(_) => todo!(),
            OclNode::EnumMember(_, _) => unreachable!(),
            OclNode::Literal(_) => todo!(),
        }
    }

    #[cfg(test)]
    mod test;
}

/*
* LINTS
* Class A has 1 implicitly named and 1 explicitly named relationship to the same Class B -> Should
* these relationships be merged?
*/

#[derive(Debug)]
enum OclBool {
    Equals(OclNode, OclNode),
    NotEquals(OclNode, OclNode),
    GreaterThan(OclNode, OclNode),
    LessThan(OclNode, OclNode),

    // Can't statically determine whether a Navigate returns an object or a collection, so have to
    // take OclNode
    Includes(OclNode, OclNode),
    IncludesAll(OclNode, OclNode),
    Excludes(OclNode, OclNode),
    ExcludesAll(OclNode, OclNode),
    IsEmpty(OclNode),
    Not(Box<OclBool>),
}

type EnumName = String;
type MemberName = String;
#[derive(Debug)]
enum OclNode {
    IterVariable(String, OclType),           // -> Output::Object
    ContextVariable(String, OclType),        // -> Output::Object
    AllInstances(ClassName),                 // -> Output::Bag
    Navigate(Box<OclNode>, FieldName),       // -> Output::Object | Output::Bag
    BasicAttribute(Box<OclNode>, FieldName), // -> Output::Field
    Select(HashMap<String, OclType>, Box<OclNode>, Box<OclNode>), // Output::Bag
    First(Box<OclNode>),
    Count(Box<OclNode>), // Output::Count
    Bool(Box<OclBool>),  // Output::Bool
    Literal(OclLiteral),
    EnumMember(EnumName, MemberName), // Output::Constant
}

fn typecheck(ocl: &OclNode, model: &model::Model) -> OclType {
    match ocl {
        OclNode::EnumMember(enum_name, _) => OclType::Basic(Basic::Enum(enum_name.clone())),
        OclNode::IterVariable(_, typ) => typ.clone(),
        OclNode::ContextVariable(_, typ) => typ.clone(),
        OclNode::AllInstances(name) => OclType::Class(name.clone()),
        OclNode::Navigate(node, nav) => {
            let typ = typecheck(node, model);

            match typ {
                OclType::Basic(_) => todo!("Figure out if primitives have navigations"),
                OclType::Class(name) => {
                    let class = &model.classes[&name];
                    model.field_of(class, nav).unwrap()
                }
            }
        }
        OclNode::BasicAttribute(node, nav) => {
            let typ = typecheck(node, model);

            match typ {
                OclType::Basic(_) => panic!("Can't get the field of a primitive"),
                OclType::Class(name) => {
                    let class = &model.classes[&name];
                    match class.primitive_fields.get(nav) {
                        None => todo!("Get parent class fields"),
                        Some(p) => OclType::Basic(p.clone()),
                    }
                }
            }
        }
        OclNode::Select(_, node, _) =>
        /* Bag(typecheck(node, model).innertype()) */
        {
            typecheck(node, model)
        }
        OclNode::First(node) => typecheck(node, model),
        OclNode::Count(_) => OclType::Basic(Basic::Integer),
        OclNode::Bool(_) => OclType::Basic(Basic::Boolean),
        OclNode::Literal(literal) => OclType::Basic(match literal {
            OclLiteral::Real(_) => Basic::Real,
            OclLiteral::Integer(_) => Basic::Integer,
            OclLiteral::String(_) => Basic::String,
            OclLiteral::Boolean(_) => Basic::Boolean,
        }),
    }
}

mod query_parser;

type Context = TableAlias;

#[derive(Default)]
struct OclContext {
    parameters: HashMap<String, Context>,
    iterator_vars: Vec<HashMap<String, Context>>,
}

enum Resolution {
    IterVar(Context),
    Parameter(Context),
    NotFound,
}

impl Resolution {
    fn unwrap_iter(self) -> Context {
        match self {
            Resolution::IterVar(c) => c,
            Resolution::Parameter(_) => {
                panic!("Iter variable not found but context var with same name was found")
            }
            Resolution::NotFound => panic!("Iter variable not found"),
        }
    }

    fn unwrap_parameter(self) -> Context {
        match self {
            Resolution::Parameter(c) => c,
            Resolution::IterVar(_) => {
                panic!("Context variable not found but iter var with same name was found")
            }
            Resolution::NotFound => panic!("Context variable not found"),
        }
    }
}

impl OclContext {
    fn resolve(&self, name: &str) -> Resolution {
        for iter in self.iterator_vars.iter().rev() {
            if let Some(t) = iter.get(name) {
                return Resolution::IterVar(t.clone());
            }
        }

        if let Some(c) = self.parameters.get(name) {
            Resolution::Parameter(c.clone())
        } else {
            Resolution::NotFound
        }
    }

    fn push_iter(&mut self, hm: HashMap<String, Context>) {
        self.iterator_vars.push(hm);
    }

    fn pop_iter(&mut self) {
        self.iterator_vars.pop();
    }
}

fn main() {
    let model = {
        let mut f = File::open("hospital.schema").unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s);
        model::parse_schema(&s).unwrap()
    };

    let stdin = io::stdin();
    let mut buf = String::new();
    let mut ocl = None;
    let mut sql = None;
    loop {
        buf.clear();
        print!("> ");
        io::stdout().lock().flush().unwrap();
        stdin.lock().read_line(&mut buf).unwrap();
        buf.truncate(buf.trim_end().len());
        let line = buf.as_str();

        if line == "" {
            break;
        } else if line == ".schema" {
            println!("{:#?}", model);
        } else if line == ".sqlschema" {
            println!("{}", model.to_schema());
        } else if line == ".ocl" {
            match &ocl {
                Some(tree) => println!("{:#?}", tree),
                None => println!("No OCL has been parsed yet"),
            }
        } else if line == ".sql" {
            match &sql {
                Some(query) => println!("{}", query),
                None => println!("No SQL has been generated yet"),
            }
        } else {
            let (generated, params) = query_parser::parse_full_query(&line, &model);
            ocl = Some(generated);
            sql = Some(sql_tree::ocl_to_sql(ocl.as_ref().unwrap(), &params, &model));
            println!("{}", sql.as_ref().unwrap());
        }
    }

    /*
    Surgery.allInstances->select(s: Surgery | s.head_surgeon.patients->size() < 10)

    SELECT Surgery_class_1.* FROM
        Surgery_class AS Surgery_class_1
        JOIN Doctor_class as Doctor_class_1 ON Surgery_class_1.Surgery_head_surgeon = Doctor_class_1.id
        JOIN Kit_class as Patient_class_1 ON Patient_class_1.Patient_doctor = Doctor_class_1.id
    GROUP BY Doctor_class_1.id
    HAVING Count(Patient_class_1.id) < 10;
     */

    /*
     Surgery.allInstances->select(s: Surgery | s.head_surgeon.patients->size() < s.normal_doctors->size())
    SELECT Surgery_class_1.* FROM
        Surgery_class AS Surgery_class_1
        JOIN Doctor_class as Doctor_class_1 ON Surgery_class_1.Surgery_head_surgeon = Doctor_class_1.id
        JOIN Kit_class as Patient_class_1 ON Patient_class_1.Patient_doctor = Doctor_class_1.id
        JOIN Doctor_class_normal_doctors_Surgery_class_normal_surgeries AS Doctor_class_normal_doctors_Surgery_class_normal_surgeries_1 ON Doctor_class_normal_doctors_Surgery_class_normal_surgeries_1.Doctor_normal_surgeries = Surgery_class_1.id
        JOIN Doctor_class as Doctor_class_2 ON Doctor_class_2.id = Doctor_class_normal_doctors_Surgery_class_normal_surgeries_1.Surgery_normal_doctors
    GROUP BY Doctor_class_1.id, Doctor_class_2.id
    HAVING Count(Patient_class_1.id) < Count(Doctor_class_2.id);

     */

    println!("OCL text: \ncontext self: <some external value>\nself.hospital.doctors->size()");
    let (ocl, mut context) = query_parser::parse_full_query(
        "params self: Doctor in: self.hospital.doctors->size()",
        &model,
    );
    println!("OCL tree: {ocl:?}");

    let sql = sql_tree::ocl_to_sql(&ocl, &context, &model);

    println!("Generated query: {}", sql);

    println!("query: self.hospital.doctors->select(o: Person | o.age < self.age)");
    let (age_ocl, age_context) = query_parser::parse_full_query(
        "parameters self: Patient in: self.hospital.doctors->select(o: Person | o.age < self.age)",
        &model,
    );

    let sql = sql_tree::ocl_to_sql(&age_ocl, &age_context, &model);
    println!("Generated query: {}", sql);

    let q = "1";
    let (ocl, context) = query_parser::parse_full_query(q, &model);
    let sql = sql_tree::ocl_to_sql(&ocl, &context, &model);
    println!("query: {q}\n generated: {sql}");
    /*
    // query for all patients whose emergency contact is a doctor at the same hospital
    context Patient
    query: Patient.allInstances->select(p: Patient | p.hospital.doctors->includes(p.emergency_contact))
     */
    /*
    Navigate(object, path: Type)
    Select(AllInstances(Patient): *Patient, setElem: Patient | Includes(Navigate(Navigate(setElem: Patient, hospital: Hospital): Hospital, doctors: *Doctor), Navigate(setElem, emergency_contact: Person))
    Select(set, expr) -> SELECT ... WHERE expr
    Includes(set, elem) -> WHERE rowid = elem
    setElem (type, id) -> SELECT * FROM type_table WHERE rowid = id

    Navigate(One, One) -> JOIN other_table ON other_table.rowid = this_table.navigation
    Navigate(Many, One) -> JOIN other_table ON other_table.rowid = this_table.navigation
    Navigate(One, Many) ->
        find the navigation on the other end
        JOIN other_table ON other_table.other_navigation = this_table.rowid
    Navigate(Many, Many)
        // FIXM
        find the navigation on the other end
        JOIN other_table ON other_table.other_navigation = this_table.rowid
    AllInstances(class) -> SELECT * FROM class
     */
    /*
    SELECT <Patient fields> FROM Patient_class JOIN Doctor_class ON Patient_class.hospital = Doctor_class.hospital WHERE Patient_class.emergency_contact = Doctor_class.id;
     */
}

/*
 * // Doctors named Dave who are not emergency contacts at my hospital
 * self.hospital.doctors->select( d: Person |
 *  self.hospital.patients.emergency_contact.excludes(d)).name.select( s: String |
 *    s == "Dave"
 *  )
 *
 *  self.hospital.doctors->select(d1: Doctor |
 *      d1.name == "Dave"
 *  ).surgeries.doctors->select(d2: Doctor |
 *      d2 <> d1 and d2.hospital = d1.hospital
 *  )->first().hospital.patients
 *
 *  SELECT patients1.* FROM
 *  (
 *      SELECT doctors2.* FROM
 *         Doctor AS self
 *         JOIN Doctor AS doctors1 ON doctors1.hospital = self.hospital
 *         JOIN Person AS doctors1_person ON doctors1.id = doctors1_person.id
 *         JOIN SurgeriesDoctorsAssoc AS surgery_assoc ON doctors1.id = surgery_assoc.doctor
 *         JOIN Doctors AS doctors2 ON doctor2.id = surgery_assoc.doctor
 *     WHERE
 *         self.id = $self AND
 *         doctors1_person.name = "Dave" AND
 *         doctors1.id != doctors2.id AND
 *         doctors1.hospital = doctors2.hospital
 *      ORDER BY ????????
 *      LIMIT 1
 * ) AS doctors3 JOIN Patient AS patients1 ON doctors3.hospital = patients1.hospital
 *
 *
Output::Class(
    "patients1",
    Join(
        Named(
            Output::Class(
                "doctors2",
                Join(
                    Join(
                        Join(
                            Join(
                                Named("Doctor", "self"),
                                Named("Doctor", "doctors1"),
                                ("doctors1", "hospital", "self", "hospital")
                            ),
                            Named(Person, doctors1_person),
                            (doctors1, id, doctors1_person, id)
                        ),
                        Named(SurgeriesDoctorsAssoc, surgery_assoc),
                        (doctors1, id, surgery_assoc, doctor)
                    )
                    Named(Doctors, doctors2),
                    (doctor2, id, surgery_assoc, doctor)
                )
                And(
                    Equals(Field("self", "id"), Parameter("self")),
                    And(
                        Equals(Field("doctors1_person", "name"), Constant("Dave")),
                        And(
                            NotEquals(Field("doctors1", "id"), Field("doctors2", id)),
                            Equals(Field("doctors1", "hospital"), Field("doctors2", "hospital"))
                        )
                    )
                ),
                Limit(1)
            ),
            "doctors3"
        ),
        Named("Patient", "patients1"),
        ("doctors3", "hospital", "patients1", "hospital")
    ),
    NoWhereClause
)
 *
 * If an operation would ORDER LIMIT or GROUP a specific table, and it is not the last operation in
 * the query, then everything up to this point becomes a sub query and the ORDER LIMIT or GROUP is
 * applied inside that query, then processing continues as usual, joining on the result of the sub
 * query
 */
