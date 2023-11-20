use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::marker::PhantomData;
use std::rc::Rc;

use model::canonicalize;

use crate::model::ClassBuilder;
use crate::SqlTree::test_tree;

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

#[derive(Debug, Clone)]
pub enum Primitive {
    Real,
    Integer,
    String,
    Boolean,
    Enum(String), // TODO Enum(...)
}

// TODO: Mark whether the type is single or a collection
#[derive(Clone, Debug)]
pub enum OclType {
    Primitive(Primitive),
    Class(ClassName),
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

    enum Expr {
        Field(TableAlias, ColumnName),
        Constant(Constant),
        Parameter(String),
    }

    impl std::fmt::Display for Expr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            match self {
                Expr::Field(table, column) => write!(f, "{}.{}", table.escape(), column.escape()),
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

    use std::{fmt::Display, num::NonZeroUsize};

    use crate::{ColumnName, TableAlias, TableName};

    // FIXME this should include a field descriptor too
    enum Output {
        Table(TableAlias),
        Count(TableAlias),
    }

    impl Display for Output {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Output::Table(alias) => write!(f, "{}", alias.escape()),
                Output::Count(alias) => write!(f, "COUNT({}.id)", alias.escape()),
            }
        }
    }

    struct Query {
        output: Output,
        body: Body,
        r#where: Option<BoolCondition>,
        limit: Option<NonZeroUsize>,
    }

    impl Display for Query {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SELECT {} FROM {}", self.output, self.body)?;

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

    pub fn test_tree() {
        let tree = Query {
            output: Output::Table("patients1".into()),
            body: Body::Join(
                Box::new(Body::Named(
                    Table::Query(Box::new(Query {
                        output: Output::Table("doctors2".into()),
                        body: Body::Join(
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
                                            Expr::Field("doctors1".into(), "hospital".into()),
                                            Expr::Field("self".into(), "hospital".into()),
                                        ),
                                    )),
                                    Box::new(Body::Named(
                                        Table::Table("Person".into()),
                                        "doctors1_person".into(),
                                    )),
                                    BoolCondition::Equal(
                                        Expr::Field("doctors1".into(), "id".into()),
                                        Expr::Field("doctors1_person".into(), "id".into()),
                                    ),
                                )),
                                Box::new(Body::Named(
                                    Table::Table("SurgeriesDoctorsAssoc".into()),
                                    "surgery_assoc".into(),
                                )),
                                BoolCondition::Equal(
                                    Expr::Field("doctors1".into(), "id".into()),
                                    Expr::Field("surgery_assoc".into(), "doctor".into()),
                                ),
                            )),
                            Box::new(Body::Named(
                                Table::Table("Doctors".into()),
                                "doctors2".into(),
                            )),
                            BoolCondition::Equal(
                                Expr::Field("doctors2".into(), "id".into()),
                                Expr::Field("surgery_assoc".into(), "doctor".into()),
                            ),
                        ),
                        r#where: Some(BoolCondition::And(
                            Box::new(BoolCondition::Equal(
                                Expr::Field("self".into(), "id".into()),
                                Expr::Parameter("self".into()),
                            )),
                            Box::new(BoolCondition::And(
                                Box::new(BoolCondition::Equal(
                                    Expr::Field("doctors1_person".into(), "name".into()),
                                    Expr::Constant("Dave".into()),
                                )),
                                Box::new(BoolCondition::And(
                                    Box::new(BoolCondition::NotEqual(
                                        Expr::Field("doctors1".into(), "id".into()),
                                        Expr::Field("doctors2".into(), "id".into()),
                                    )),
                                    Box::new(BoolCondition::Equal(
                                        Expr::Field("doctors1".into(), "hospital".into()),
                                        Expr::Field("doctors2".into(), "hospital".into()),
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
                    Expr::Field("doctors3".into(), "hospital".into()),
                    Expr::Field("patients1".into(), "hospital".into()),
                ),
            ),
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
    IterVariable(String),                    // -> Output::Object
    ContextVariable(String),                 // -> Output::Object
    AllInstances(ClassName),                 // -> Output::Bag
    Navigate(Box<OclNode>, FieldName),       // -> Output::Object | Output::Bag
    PrimitiveField(Box<OclNode>, FieldName), // -> Output::Field
    Select(HashMap<String, OclType>, Box<OclNode>, Box<OclNode>), // Output::Bag
    Count(Box<OclNode>),                     // Output::Count
    Bool(Box<OclBool>),                      // Output::Bool
    EnumMember(EnumName, MemberName),        // Output::Constant
}

fn typecheck(ocl: &OclNode, ctx: &OclContext, model: &model::Model) -> OclType {
    match ocl {
        OclNode::EnumMember(enum_name, _) => OclType::Primitive(Primitive::Enum(enum_name.clone())),
        OclNode::IterVariable(name) => ctx.resolve(name).unwrap_iter().0,
        OclNode::ContextVariable(name) => ctx.resolve(name).unwrap_context().0,
        OclNode::AllInstances(name) => OclType::Class(name.clone()),
        OclNode::Navigate(node, nav) => {
            let typ = typecheck(node, ctx, model);

            match typ {
                OclType::Primitive(_) => todo!("Figure out if primitives have navigations"),
                OclType::Class(name) => {
                    let class = &model.classes[&name];
                    model.field_of(class, nav).unwrap()
                }
            }
        }
        OclNode::PrimitiveField(node, nav) => {
            let typ = typecheck(node, ctx, model);

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
        OclNode::Select(vars, node, _) => typecheck(node, todo!("ctx with added vars"), model),
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
    fn with_context(map: HashMap<String, Context>) -> Self {
        OclContext {
            parameters: map,
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

    fn push_iter(&mut self, hm: HashMap<String, Context>) {
        self.iterator_vars.push(hm);
    }

    fn pop_iter(&mut self) {
        self.iterator_vars.pop();
    }
}

struct Query;

type Bindings = HashMap<String, TableAlias>;

/*
 * When a collection is reduced to a single value type, start a new query and feed that single value as
 * the new base table
 *
 * AKA
 *
 * when an operation would set the output but isn't the last operation, needs to be a sub query
 */

fn build_query(
    ocl: &OclNode,
    model: &model::Model,
    sql: &mut SQLQueryBuilder,
    context: &mut OclContext,
) -> (TableAlias, ClassName) {
    match ocl {
        OclNode::IterVariable(name) => {
            todo!(
                "keep track of the alias of the currently iterating collection

            if the variable is an upcast, add a join onto the base class"
            )
        }
        OclNode::ContextVariable(name) => {
            let Resolution::ContextVar((typ, _)) = context.resolve(name) else {
                panic!("Unknown variable")
            };

            // FIXME: Variable could already be bound to a table alias, in which case just use
            // that?
            // Only possible if that alias is not already being restricted by a WHERE clause or something
            let tbl = match typ {
                OclType::Class(ident) => ident,
                _ => todo!("Non-object variables"),
            };
            let alias = sql.add_base_table(model::canonicalize::class_table_name(&tbl));
            sql.add_where(BoolCondition::Equal(
                Expr::Field(alias.clone(), "id".into()),
                Expr::SqlParameter(name.clone()),
            ));
            (alias, tbl)
        }
        OclNode::AllInstances(tbl) => {
            let alias = sql.add_base_table(model.table_of(tbl));
            (alias, tbl.clone())
        }
        OclNode::Select(iter_vars, node, condition) => {
            // TODO: Collect the types of the Iter variable
            // if the type is not a supertype then error
            // Otherwise the upcasting should Just Work™
            // Downcasting is not allowed
            let (output_tbl, output_typ) = build_query(node, model, sql, context);

            let vars_with_aliases = iter_vars
                .iter()
                .map(|(varname, vartype)| {
                    (varname.clone(), (vartype.clone(), Some(output_tbl.clone())))
                })
                .collect();

            context.push_iter(vars_with_aliases);

            //set current iter collection to the output_tbl

            if let OclNode::Bool(bool) = &**condition {
                match &**bool {
                    OclBool::LessThan(node1, node2) => {
                        let lhs = build_query(node1, model, sql, context);
                        let rhs = build_query(node2, model, sql, context);
                        todo!("Extract the field name from the query");
                        sql.add_where(BoolCondition::Lt(todo!(), todo!()));
                        return (output_tbl, output_typ);
                    }
                    _ => todo!(),
                }
            } else {
                assert!(matches!(
                    typecheck(condition, context, model),
                    OclType::Primitive(Primitive::Boolean)
                ));
                todo!("We need to do some magic joining and then WHERE result.bool_field = 1");
            }
            context.pop_iter();
            todo!()
        }
        OclNode::Count(node) => {
            let output_tbl = build_query(node, model, sql, context);
            let table_name = &output_tbl.0;
            sql.output = Some(Output::Count(table_name.clone(), "id".into()));
            output_tbl
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

            let (prev_alias, prev_type) = build_query(node, model, sql, context);

            // TODO: check for primitive field navigations

            let assoc = &model.classes[&prev_type].associations[navigation];

            // TODO: If navigating to a class contained within the same SQL table (i.e. navigation is a true 1-1 association), just return prev_alias
            match assoc.associativity {
                Associativity::One => {
                    let new_type = assoc.target.clone();
                    let new_table = model.table_of(&assoc.target);
                    let field_name = canonicalize::column_name(prev_type, &navigation);
                    let new_alias =
                        sql.add_joined_table((prev_alias, field_name), (new_table, "id".into()));
                    (new_alias, new_type)
                }
                Associativity::Many => {
                    let rev = assoc.reverse();
                    let new_type = assoc.target.clone();
                    let new_table = model.table_of(&assoc.target);
                    let field_name = canonicalize::column_name(new_type.clone(), &rev.navigation);
                    let new_alias = match rev.associativity {
                        Associativity::One => {
                            sql.add_joined_table((prev_alias, "id".into()), (new_table, field_name))
                        }
                        x => todo!("{:?}", x),
                    };
                    (new_alias, new_type)
                }
                _ => todo!(),
            }

            //add table
            //add join to prev_alias
            //if it's a many-many then add additional target table and join
        }
        OclNode::Bool(_) => todo!(),
        OclNode::EnumMember(_, _) => todo!(),
    }
}

fn main() {
    test_tree();
    panic!();
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
    // let mut context = OclContext::with_context(HashMap::from([(
    //     "self".to_string(),
    //     (OclType::Class("Doctor".into()), None),
    // )]));
    // let ocl = OclNode::Count(
    //     OclNode::Navigate(
    //         OclNode::Navigate(
    //             OclNode::ContextVariable("self".into()).into(),
    //             "hospital".into(),
    //         )
    //         .into(),
    //         "doctors".into(),
    //     )
    //     .into(),
    // );
    println!("OCL tree: {ocl:?}");

    let mut sql = SQLQueryBuilder::default();

    build_query(&ocl, &model, &mut sql, &mut context);
    println!("Generated query: {}", sql.to_sqlite3());

    let mut sql = SQLQueryBuilder::default();
    // query: self.hospital.doctors->select(o: Person | o.age < self.age)
    let (age_ocl, mut age_context) = query_parser::parse_full_query(
        "bikeshed self: Patient in: 
        self.hospital.doctors->select(o: Person | o.age < self.age)
    ",
        &model,
    );
    // let mut age_context = OclContext::with_context(HashMap::from([(
    //     "self".into(),
    //     (OclType::Class("Doctor".into()), None),
    // )]));

    // let age_ocl = OclNode::Select(
    //     HashMap::from([("p".into(), OclType::Class("Person".into()))]),
    //     OclNode::Navigate(
    //         OclNode::Navigate(
    //             OclNode::ContextVariable("self".into()).into(),
    //             "hospital".into(),
    //         )
    //         .into(),
    //         "doctors".into(),
    //     )
    //     .into(),
    //     Box::new(
    //         todo!(), /*OclBool::LessThan(
    //                      OclNode::Navigate(Box::new(OclNode::IterVariable("p".into())), "age".into()),
    //                      OclNode::Navigate(
    //                          Box::new(OclNode::ContextVariable("self".into())),
    //                          "age".into(),
    //                      ),
    //                  )*/
    //     ),
    // );
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
    dbg!(&age_ocl);
    build_query(&age_ocl, &model, &mut sql, &mut age_context);
    println!("Generated query: {}", sql.to_sqlite3());
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
