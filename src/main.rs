use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::marker::PhantomData;
use std::rc::Rc;

use model::canonicalize;

use crate::model::ClassBuilder;
use crate::SqlTree::{ocl_to_sql, test_tree};

mod model;

mod name {
    use std::marker::PhantomData;

    pub struct Name<T>(String, PhantomData<T>);

    pub trait NameType {}
    pub trait SqlNameType {}

    pub struct ClassName;
    impl NameType for ClassName {}

    pub struct FieldName;
    impl NameType for FieldName {}

    pub struct SqlIdentifier;
    impl NameType for SqlIdentifier {}
    impl SqlNameType for SqlIdentifier {}

    impl<T> From<&str> for Name<T> {
        fn from(s: &str) -> Self {
            Name(s.to_string(), PhantomData)
        }
    }

    impl<T: NameType> Name<T> {
        fn from_string(s: String) -> Name<T> {
            Name(s, PhantomData)
        }
        fn into_inner(self) -> String {
            self.0
        }

        fn as_str(&self) -> &str {
            &self.0
        }
    }

    impl<T: SqlNameType> Name<T> {
        fn escape(&self) -> String {
            format!("\"{}\"", self.0.replace("\"", "\"\""))
        }
    }
}

// TODO: TypeState this to reduce impl redundancy
#[derive(Debug, Hash, PartialOrd, Ord, Eq, PartialEq, Clone)]
pub struct SqlIdentifier(String);

impl From<&str> for SqlIdentifier {
    fn from(s: &str) -> Self {
        SqlIdentifier(s.to_string())
    }
}

impl From<String> for SqlIdentifier {
    fn from(value: String) -> Self {
        SqlIdentifier(value)
    }
}

impl SqlIdentifier {
    fn into_inner(self) -> String {
        self.0
    }

    fn escape(&self) -> String {
        format!("\"{}\"", self.0.replace("\"", "\"\""))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClassName(String);

impl From<&str> for ClassName {
    fn from(s: &str) -> Self {
        ClassName(s.to_string())
    }
}

impl ClassName {
    fn into_inner(self) -> String {
        self.0
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct FieldName(String);

impl From<&str> for FieldName {
    fn from(s: &str) -> Self {
        FieldName(s.to_string())
    }
}

impl FieldName {
    fn into_inner(self) -> String {
        self.0
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Primitive {
    Real,
    Integer,
    String,
    Boolean,
    Enum(String), // TODO Enum(...)
}

// TODO: Mark whether the type is single or a collection
#[derive(Clone, Debug, PartialEq)]
pub enum OclType {
    Primitive(Primitive),
    Class(ClassName),
}

impl OclType {
    fn class_name(&self) -> &ClassName {
        match self {
            OclType::Primitive(_) => panic!("No class name for primitive"),
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

type TableAlias = SqlIdentifier;
type TableName = SqlIdentifier;
type ColumnName = SqlIdentifier;

#[non_exhaustive]
enum Output {
    PrimitiveField(TableAlias, ColumnName),
    Class(TableAlias), //TODO: assert that returned set has 1 row
    Count(TableAlias, ColumnName),
    Bag(TableAlias),
    // TODO: Further aggregates
}

impl std::fmt::Display for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Output::PrimitiveField(table, column) => {
                write!(f, "{}.{}", table.escape(), column.escape())
            }
            Output::Class(table) => write!(f, "{}.*", table.escape()),
            Output::Count(table, column) => {
                write!(f, "COUNT({}.{})", table.escape(), column.escape())
            }
            Output::Bag(table) => write!(f, "{}.*", table.escape()),
            _ => todo!(),
        }
    }
}

enum Constant {
    Real(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
}

enum Expr {
    SubQuery(()),
    Bool(Box<BoolCondition>),
    Field(TableAlias, ColumnName),
    Constant(Constant),
    SqlParameter(String),
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Expr::SubQuery(()) => todo!(),
            Expr::Bool(cond) => write!(f, "{cond}"),
            Expr::Field(table, column) => write!(f, "{}.{}", table.escape(), column.escape()),
            Expr::Constant(c) => todo!(),
            Expr::SqlParameter(name) => write!(f, "${}", name),
        }
    }
}

enum BoolCondition {
    And(Box<BoolCondition>, Box<BoolCondition>),
    Equal(Expr, Expr),
    Gt(Expr, Expr),
    Lt(Expr, Expr),
}

impl std::fmt::Display for BoolCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            BoolCondition::And(a, b) => write!(f, "{a} AND {b}"),
            BoolCondition::Equal(a, b) => write!(f, "{a} = {b}"),
            BoolCondition::Gt(a, b) => write!(f, "{a} > {b}"),
            BoolCondition::Lt(a, b) => write!(f, "{a} < {b}"),
        }
    }
}

// TODO: Just stuff these all in the Where clause?
type Join = ((TableAlias, ColumnName), (TableAlias, ColumnName));

// TODO: Typestate this so a base_table and output are always added

mod SqlTree {
    enum Constant {
        Real(f64),
        Integer(i64),
        String(String),
        Boolean(bool),
    }

    impl Display for Constant {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Constant::Real(_) => todo!(),
                Constant::Integer(_) => todo!(),
                Constant::String(s) => write!(f, "{s}"), //FIXME: Prevent SQL injection
                Constant::Boolean(_) => todo!(),
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

    impl From<bool> for Constant {
        fn from(value: bool) -> Self {
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
                Expr::Constant(c) => write!(f, "'{c}'"),
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
        model::canonicalize, typecheck, Associativity, ClassName, ColumnName, OclBool, OclContext,
        OclNode, OclType, Primitive, Resolution, TableAlias, TableName,
    };

    // FIXME this should include a field descriptor too
    enum Output {
        Table(Expr),
        Count(TableAlias),
    }

    impl Display for Output {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Output::Table(e) => write!(f, "{}", e),
                Output::Count(alias) => write!(f, "COUNT({}.id)", alias.escape()),
            }
        }
    }

    impl InProgressQuery {
        fn to_count(self, table: TableAlias, columnspec: ColumnName) -> Query {
            Query {
                output: Output::Count(table),
                body: Some(self.body.unwrap()),
                r#where: self.r#where,
                limit: None,
            }
        }

        fn to_select_table(self, table: TableAlias, column: ColumnSpec) -> Query {
            self.to_select(Expr::Field(table, column))
        }

        fn to_select(self, e: Expr) -> Query {
            if matches!(e, Expr::Field(_, _)) {
                assert!(self.body.is_some());
            }
            Query {
                output: Output::Table(e),
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
                output: Output::Table(Expr::Field(table, columnspec)),
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
            write!(name, "_{}", n);
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

    enum NodeType {
        Base,
        NonBase,
    }

    fn is_base(node: &OclNode) -> bool {
        match node {
            OclNode::IterVariable(_, _) => true,
            OclNode::ContextVariable(_, _) => true,
            OclNode::AllInstances(_) => true,
            OclNode::Navigate(_, _) => todo!(),
            OclNode::PrimitiveField(_, _) => todo!(),
            OclNode::Select(_, _, _) => todo!(),
            OclNode::Count(_) => todo!(),
            OclNode::Bool(_) => todo!(),
            OclNode::EnumMember(_, _) => true,
        }
    }

    #[derive(Default)]
    struct QueryState {
        aliases: HashMap<String, usize>,
        bindings: HashMap<
            String,
            (
                /* TODO: Put the OclType into the parse tree */ OclType,
                TableAlias,
            ),
        >,
        upcasts: HashMap<(TableAlias, ClassName), TableAlias>, // Keep track of the alias of the upcast table for the given table/type
    }

    // TODO: Just pass in an immutable list of context arg types
    pub(crate) fn ocl_to_sql(
        ocl: &OclNode,
        parameters: &HashMap<String, OclType>,
        model: &crate::model::Model,
    ) -> Query {
        // TODO: Return a Query and a ParameterInfo struct to support non sqlite parameters
        let mut aliases = Aliases::default();
        let mut context = OclContext::with_parameters(parameters);
        let mut state: QueryState = Default::default();
        // match the top-level node to determine output
        match ocl {
            OclNode::IterVariable(_, _) => panic!("Invalid OCL query"),
            OclNode::ContextVariable(varname, _) => {
                let (typ, _) = context.resolve(varname).unwrap_context();

                match typ {
                    OclType::Class(_) => todo!("build_query"),
                    OclType::Primitive(p) => Query {
                        output: Output::Table(Expr::Parameter(varname.to_string())),
                        body: None,
                        r#where: None,
                        limit: None,
                    },
                }
            }
            OclNode::AllInstances(_) => todo!(),
            OclNode::Navigate(_, _) => {
                let in_progress = InProgressQuery::default();
                let (in_progress, table) =
                    build_query(ocl, model, in_progress, &mut context, &mut aliases);

                in_progress.to_select_table(table, todo!())
            }
            OclNode::PrimitiveField(node, field) => {
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
            OclNode::Count(node) => {
                let in_progress = InProgressQuery::default();
                let (in_progress, table) =
                    build_query(node, model, in_progress, &mut context, &mut aliases);

                let columnspec = "*".into();
                in_progress.to_count(table, columnspec)
            }
            OclNode::Bool(_) => todo!(),
            OclNode::EnumMember(name, member) => {
                let Some(constant) = model.enums[name].index_of(member) else {
                    panic!("Invalid field {member:?} for enum {name}")
                };
                Query {
                    output: Output::Table(Expr::Constant(Constant::Integer(constant as i64))),
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
                (sql, var.unwrap_iter().1.unwrap())

                // if the variable is an upcast, add a join onto the base class
            }
            OclNode::ContextVariable(name, _) => {
                let Resolution::ContextVar((typ, binding)) = context.resolve(name) else {
                    panic!("Unknown variable")
                };

                let alias = match binding {
                    Some(alias) => alias,
                    None => {
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

                        context.resolve_mut(name).unwrap().1 = Some(alias.clone());

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
                    let binding = Some(top_of_stack_alias.clone());

                    vars_with_aliases.insert(varname.clone(), (vartype.clone(), binding));
                }

                context.push_iter(vars_with_aliases);

                //

                //set current iter collection to the output_tbl

                if let OclNode::Bool(bool) = &**condition {
                    match &**bool {
                        OclBool::LessThan(node1, node2) => {
                            let left_expr: Expr;
                            let right_expr: Expr;

                            let lhs = if let OclNode::PrimitiveField(node, field) = node1 {
                                let typ: OclType;
                                let ret_sql;

                                let alias = if let OclNode::IterVariable(varname, _) = &**node {
                                    let (ty, alias) = context.resolve(varname).unwrap_iter();
                                    typ = ty;
                                    ret_sql = sql;
                                    alias.unwrap()
                                } else {
                                    let alias;
                                    (ret_sql, alias) =
                                        build_query(node, model, sql, context, aliases);
                                    typ = typecheck(node, model);
                                    alias
                                };

                                let OclType::Class(class) = typ else { panic!() };
                                //todo!("need to check if field is on parent class");
                                let columnspec = ColumnSpec::Named(
                                    crate::model::canonicalize::column_name(class, field),
                                );
                                left_expr = Expr::Field(alias, columnspec);
                                ret_sql
                            } else {
                                todo!()
                            };

                            let rhs = if let OclNode::PrimitiveField(node, field) = node2 {
                                let typ: OclType;
                                let ret_sql;
                                // TODO: If node == IterVar, don't build_query just reuse the alias
                                let alias = if let OclNode::IterVariable(varname, _) = &**node {
                                    let (ty, alias) = context.resolve(varname).unwrap_iter();
                                    typ = ty;
                                    ret_sql = lhs;
                                    alias.unwrap()
                                } else {
                                    let alias;
                                    (ret_sql, alias) =
                                        build_query(node, model, lhs, context, aliases);
                                    typ = typecheck(node, model);
                                    alias
                                };

                                let OclType::Class(class) = typ else { panic!() };
                                let columnspec = ColumnSpec::Named(
                                    crate::model::canonicalize::column_name(class, field),
                                );
                                right_expr = Expr::Field(alias, columnspec);
                                ret_sql
                            } else {
                                todo!()
                            };

                            let mut sql = rhs;
                            sql.add_where(BoolCondition::LessThan(left_expr, right_expr));
                            return (sql, top_of_stack_alias);
                        }
                        _ => todo!(),
                    }
                } else {
                    assert!(matches!(
                        typecheck(condition, model),
                        OclType::Primitive(Primitive::Boolean)
                    ));
                    todo!("We need to do some magic joining and then WHERE result.bool_field = 1");
                }
                context.pop_iter();
                todo!()
            }
            OclNode::Count(node) => {
                let (sql, table_name) = build_query(node, model, sql, context, aliases);
                let subquery = sql.to_count(table_name.clone(), "id".into());
                let alias = aliases.new_alias(table_name);
                (
                    InProgressQuery {
                        body: Some(Body::Named(Table::Query(Box::new(subquery)), alias.clone())),
                        r#where: None,
                        limit: None,
                    },
                    alias,
                )
            }
            OclNode::PrimitiveField(node, navigation) => {
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
        }
    }

    pub fn test_tree() {
        let tree = Query {
            output: Output::Table(Expr::Field("patients1".into(), ColumnSpec::Star)),
            body: Some(Body::Join(
                Box::new(Body::Named(
                    Table::Query(Box::new(Query {
                        output: Output::Table(Expr::Field("doctors2".into(), ColumnSpec::Star)),
                        body: Some(Body::Join(
                            Box::new(Body::Join(
                                Box::new(Body::Join(
                                    Box::new(Body::Join(
                                        Box::new(Body::Named(
                                            Table::Table("Doctor".into()),
                                            "self".into(),
                                        )),
                                        Box::new(Body::Named(
                                            Table::Table("Doctor".into()),
                                            "doctors1".into(),
                                        )),
                                        BoolCondition::Equal(
                                            Expr::Field(
                                                "doctors1".into(),
                                                ColumnSpec::Named("hospital".into()),
                                            ),
                                            Expr::Field(
                                                "self".into(),
                                                ColumnSpec::Named("hospital".into()),
                                            ),
                                        ),
                                    )),
                                    Box::new(Body::Named(
                                        Table::Table("Person".into()),
                                        "doctors1_person".into(),
                                    )),
                                    BoolCondition::Equal(
                                        Expr::Field(
                                            "doctors1".into(),
                                            ColumnSpec::Named("id".into()),
                                        ),
                                        Expr::Field(
                                            "doctors1_person".into(),
                                            ColumnSpec::Named("id".into()),
                                        ),
                                    ),
                                )),
                                Box::new(Body::Named(
                                    Table::Table("SurgeriesDoctorsAssoc".into()),
                                    "surgery_assoc".into(),
                                )),
                                BoolCondition::Equal(
                                    Expr::Field("doctors1".into(), ColumnSpec::Named("id".into())),
                                    Expr::Field(
                                        "surgery_assoc".into(),
                                        ColumnSpec::Named("doctor".into()),
                                    ),
                                ),
                            )),
                            Box::new(Body::Named(
                                Table::Table("Doctors".into()),
                                "doctors2".into(),
                            )),
                            BoolCondition::Equal(
                                Expr::Field("doctors2".into(), ColumnSpec::Named("id".into())),
                                Expr::Field(
                                    "surgery_assoc".into(),
                                    ColumnSpec::Named("doctor".into()),
                                ),
                            ),
                        )),
                        r#where: Some(BoolCondition::And(
                            Box::new(BoolCondition::Equal(
                                Expr::Field("self".into(), ColumnSpec::Named("id".into())),
                                Expr::Parameter("self".into()),
                            )),
                            Box::new(BoolCondition::And(
                                Box::new(BoolCondition::Equal(
                                    Expr::Field(
                                        "doctors1_person".into(),
                                        ColumnSpec::Named("name".into()),
                                    ),
                                    Expr::Constant("Dave".into()),
                                )),
                                Box::new(BoolCondition::And(
                                    Box::new(BoolCondition::NotEqual(
                                        Expr::Field(
                                            "doctors1".into(),
                                            ColumnSpec::Named("id".into()),
                                        ),
                                        Expr::Field(
                                            "doctors2".into(),
                                            ColumnSpec::Named("id".into()),
                                        ),
                                    )),
                                    Box::new(BoolCondition::Equal(
                                        Expr::Field(
                                            "doctors1".into(),
                                            ColumnSpec::Named("hospital".into()),
                                        ),
                                        Expr::Field(
                                            "doctors2".into(),
                                            ColumnSpec::Named("hospital".into()),
                                        ),
                                    )),
                                )),
                            )),
                        )),
                        limit: Some(NonZeroUsize::new(1).unwrap()),
                    })),
                    "doctors3".into(),
                )),
                Box::new(Body::Named(
                    Table::Table("Patient".into()),
                    "patients1".into(),
                )),
                BoolCondition::Equal(
                    Expr::Field("doctors3".into(), ColumnSpec::Named("hospital".into())),
                    Expr::Field("patients1".into(), ColumnSpec::Named("hospital".into())),
                ),
            )),
            r#where: None,
            limit: None,
        };
        println!("{tree}");
    }
}

#[derive(Default)]
struct SQLQueryBuilder {
    base_table: Option<TableAlias>,
    alias_n: HashMap<TableName, usize>,
    tables: HashMap<TableAlias, TableName>,
    joins: Vec<Join>,
    r#where: Option<BoolCondition>,
    output: Option<Output>,
}

impl SQLQueryBuilder {
    fn to_sqlite3(&self) -> String {
        let output = self.output.as_ref().expect("No output!").to_string();
        let tbls = self
            .joins
            .iter()
            .map(|((from_alias, from_col), (new_alias, new_col))| {
                let from_alias = from_alias.escape();
                let from_col = from_col.escape();
                let new_tbl = self.tables[new_alias].escape();
                let new_alias = new_alias.escape();
                let new_col = new_col.escape();
                format!("\nJOIN {new_tbl} AS {new_alias} ON {from_alias}.{from_col} = {new_alias}.{new_col}")
            })
            .collect::<String>();
        let base = self.base_table.clone().unwrap().escape();
        let base_table_name = self.tables[self.base_table.as_ref().unwrap()].escape();
        let base = format!("{base_table_name} AS {base}");
        let mut query = format!("SELECT {output} FROM {base}{tbls}");
        if let Some(w) = &self.r#where {
            query += &format!(" WHERE {w}");
        }
        query += ";";
        query
    }

    fn add_table(&mut self, name: TableName) -> TableAlias {
        let n = self.alias_n.entry(name.clone()).or_insert(0);
        let alias: TableAlias = format!("{}{n}", name.0.to_lowercase()).as_str().into();
        self.tables.insert(alias.clone(), name);
        *n += 1;
        alias
    }

    fn add_base_table(&mut self, name: TableName) -> TableAlias {
        assert!(self.base_table.is_none());

        self.base_table = Some(self.add_table(name));
        self.base_table.clone().unwrap()
    }

    fn add_joined_table(
        &mut self,
        from: (TableAlias, ColumnName),
        to: (TableName, ColumnName),
    ) -> TableAlias {
        let new_alias = self.add_table(to.0);
        self.joins.push((from, (new_alias.clone(), to.1)));
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

fn test() {
    // query: Patient.allInstances->select(p: Patient | p.hospital.doctors->includes(p.emergency_contact))
    //
    // Select(AllInstances(Patient),
    // let mut sql = SQLQueryBuilder::default();
    // sql.
    // sql.output = Output::Bag("patient1");

    // query: self.hospital.doctors->size()

    let mut sql = SQLQueryBuilder::default();
    let s = sql.add_base_table("self".into());
    let doctors = sql.add_joined_table(
        (s.clone(), "hospital".into()),
        ("Doctor_class".into(), "hospital".into()),
    );
    sql.add_where(BoolCondition::Equal(
        Expr::Field(s.clone(), "id".into()),
        Expr::SqlParameter("self".to_string()),
    ));
    sql.output = Some(Output::Count(doctors.clone(), "id".into()));
    println!("hand built: {}", sql.to_sqlite3());
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
    PrimitiveField(Box<OclNode>, FieldName), // -> Output::Field
    Select(HashMap<String, OclType>, Box<OclNode>, Box<OclNode>), // Output::Bag
    Count(Box<OclNode>),                     // Output::Count
    Bool(Box<OclBool>),                      // Output::Bool
    EnumMember(EnumName, MemberName),        // Output::Constant
}

fn typecheck(ocl: &OclNode, model: &model::Model) -> OclType {
    match ocl {
        OclNode::EnumMember(enum_name, _) => OclType::Primitive(Primitive::Enum(enum_name.clone())),
        OclNode::IterVariable(_, typ) => typ.clone(),
        OclNode::ContextVariable(_, typ) => typ.clone(),
        OclNode::AllInstances(name) => OclType::Class(name.clone()),
        OclNode::Navigate(node, nav) => {
            let typ = typecheck(node, model);

            match typ {
                OclType::Primitive(_) => todo!("Figure out if primitives have navigations"),
                OclType::Class(name) => {
                    let class = &model.classes[&name];
                    model.field_of(class, nav).unwrap()
                }
            }
        }
        OclNode::PrimitiveField(node, nav) => {
            let typ = typecheck(node, model);

            match typ {
                OclType::Primitive(_) => panic!("Can't get the field of a primitive"),
                OclType::Class(name) => {
                    let class = &model.classes[&name];
                    match class.primitive_fields.get(nav) {
                        None => todo!("Get parent class fields"),
                        Some(p) => OclType::Primitive(p.clone()),
                    }
                }
            }
        }
        OclNode::Select(vars, node, _) => typecheck(node, model),
        OclNode::Count(_) => OclType::Primitive(Primitive::Integer),
        OclNode::Bool(_) => OclType::Primitive(Primitive::Boolean),
    }
}

mod query_parser;

type Context = (OclType, Option<TableAlias>);

#[derive(Default)]
struct OclContext {
    parameters: HashMap<String, Context>,
    iterator_vars: Vec<HashMap<String, Context>>,
}

enum Resolution {
    IterVar(Context),
    ContextVar(Context),
    NotFound,
}

impl Resolution {
    fn unwrap_iter(self) -> Context {
        match self {
            Resolution::IterVar(c) => c,
            Resolution::ContextVar(_) => {
                panic!("Iter variable not found but context var with same name was found")
            }
            Resolution::NotFound => panic!("Iter variable not found"),
        }
    }

    fn unwrap_context(self) -> Context {
        match self {
            Resolution::ContextVar(c) => c,
            Resolution::IterVar(_) => {
                panic!("Context variable not found but iter var with same name was found")
            }
            Resolution::NotFound => panic!("Context variable not found"),
        }
    }
}

impl OclContext {
    fn with_parameters(map: &HashMap<String, OclType>) -> Self {
        let params = map
            .iter()
            .map(|(name, typ)| (name.clone(), (typ.clone(), None)))
            .collect();
        OclContext {
            parameters: params,
            ..OclContext::default()
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

    fn resolve_mut(&mut self, name: &str) -> Option<&mut Context> {
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

    dbg!(&model);
    println!("{}", model.to_schema());

    println!("OCL text: \ncontext self: <some external value>\nself.hospital.doctors->size()");
    let (ocl, mut context) = query_parser::parse_full_query(
        "bikeshed self: Doctor in: self.hospital.doctors->size()",
        &model,
    );
    println!("OCL tree: {ocl:?}");

    let sql = SqlTree::ocl_to_sql(&ocl, &context, &model);

    println!("Generated query: {}", sql);

    println!("query: self.hospital.doctors->select(o: Person | o.age < self.age)");
    let (age_ocl, mut age_context) = query_parser::parse_full_query(
        "bikeshed self: Patient in: 
        self.hospital.doctors->select(o: Person | o.age < self.age)
    ",
        &model,
    );
    /*

    Where = BoolCondition::And(Equals(Field(Doctor0, id), Parameter(self)), BoolCondition::And(BoolCondition::Lt(Field(Doctor1, age), Field(Doctor2, age))))




    Output::Class(
        "Doctor1",
        Join(
            Join(
                Join(
                    Named("Patient_class", "Patient0"),
                    Named("Person_class", "Person0"),
                    ("Person0", "id", "Patient0", "id")
                ),
                Named("Doctor_class", "Doctor1"),
                ("Patient0", "hospital", "Doctor1", "hospital")
            )
            Named("Person_class", "Person1"),
            ("Person1", "id", "Doctor1", "id")
        ),
        And(
            Equal(Field("Patient0", "id"), Parameter("self")),
            LessThan(Field("Doctor1", "age"), Field("Patient0", "age"))
        )
    )

    SELECT Doctor1 FROM
        Patient_class AS Patient0
        JOIN Person_class AS Person0 ON Person0.id = Patient0.id
        JOIN Doctor_class AS Doctor1 ON Patient0.hospital = Doctor1.hospital
        JOIN Person_class as Person1 ON Person1.id = Doctor1.id
    WHERE
        Patient0.id = ?self AND
        Doctor1.age < Patient0.age;
     */
    let sql = SqlTree::ocl_to_sql(&age_ocl, &age_context, &model);
    println!("Generated query: {}", sql);
    /*
    // query to get total number of doctors at my hospital
    context some doctor // Type + id
    query: self.hospital.doctors->size()

    Size(Navigate(Navigate(Object(Doctor, $self), hospital: Hopsital), doctors: *Doctor))

    SELECT COUNT(doctors.id) FROM Doctor_class as self JOIN Doctor_class as doctors ON self.hospital = doctors.hospital WHERE self.id = $self;

    fn Size(set) {
        sql.output = COUNT(set.tbl_name)
    }

    SELECT COUNT(doctors.id) FROM doctor_class AS self
        JOIN doctor_class AS doctors ON doctors.hospital = self.hospital
        WHERE self.id = $id

    fn upcast(table, supertype) {
        JOIN supertype_class ON supertype_class.rowid = table.rowid
    }

    // query to get all doctors at my hospital younger than me
    context some doctor // Type + id
    query: hospital.doctors->select(o: Person | o.age < self.age)

    Select(Navigate(Object(Doctor, $id), hospital: Hospital), doctors: *Doctor, LessThan(Navigate(set_elem, age: Integer), Navigate(Object(Doctor, $id), age: Integer)))

    SELECT doctors.* FROM doctor_class AS self
        JOIN person_class AS self_person ON self_person.rowid = self.rowid
        JOIN doctor_class AS doctors ON doctors.hospital = self.hospital
        JOIN person_class AS doctors_person ON person_class.rowid = doctors.rowid
        WHERE self.rowid = $id
            AND doctors_person.age < self_person.age

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
    test();
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
