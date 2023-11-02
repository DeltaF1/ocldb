use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::rc::Rc;

#[derive(Eq, Hash, PartialEq, Clone)]
struct ClassName<'a>(String, PhantomData<&'a mut ()>);

#[derive(Debug)]
enum Primitive {
    Real,
    Integer,
    String,
    Boolean,
    // TODO Enum(...)
}

#[derive(Debug, Copy, Clone)]
enum Associativity {
    One,
    Optional,
    Many,
}

struct NavLink<'a> {
    target: ClassName<'a>,
    navigation: Option<String>,
    associativity: Associativity,
}

struct Association<'a>(NavLink<'a>, NavLink<'a>);

impl Association<'_> {
    fn new<'a>(link1: NavLink<'a>, link2: NavLink<'a>) -> Association<'a> {
        let (link1, link2) = match (link1.associativity, link2.associativity) {
            (Associativity::One, Associativity::One) => (link1, link2),
            (_, Associativity::One) => (link2, link1),

            _ => (link1, link2),
        };
        Association(link1, link2)
    }
}

#[derive(Default)]
struct PrimitiveClassDef<'a> {
    parent: Option<ClassName<'a>>,
    primitive_fields: HashMap<String, Primitive>,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
struct Identifier(String);

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        Identifier(s.to_string())
    }
}

fn class_name(s: &str) -> Identifier {
    Identifier(s.to_string() + "_class")
}

impl Identifier {
    fn escape(&self) -> String {
        format!("\"{}\"", self.0.replace("\"", "\"\""))
    }
}

#[derive(Debug)]
struct AssocEnd {
    association: Rc<ModelAssociation>,
    which: bool,
}

impl std::ops::Deref for AssocEnd {
    type Target = ModelNavLink;

    fn deref(&self) -> &ModelNavLink {
        if self.which {
            &self.association.1
        } else {
            &self.association.0
        }
    }
}

impl AssocEnd {
    fn reverse(&self) -> &ModelNavLink {
        if self.which {
            &self.association.0
        } else {
            &self.association.1
        }
    }

    fn get_raw(&self) -> &ModelAssociation {
        &*self.association
    }
}

#[derive(Debug)]
struct ModelClass {
    parent: Option<Identifier>,
    primitive_fields: HashMap<Identifier, Primitive>,
    primitive_collections: HashMap<Identifier, (Primitive, Associativity)>,
    associations: HashMap<Identifier, AssocEnd>,
}

#[derive(Debug)]
struct ModelNavLink {
    target: Identifier,
    navigation: Identifier,
    associativity: Associativity,
}

impl From<NavLink<'_>> for ModelNavLink {
    fn from(l: NavLink) -> Self {
        ModelNavLink {
            target: class_name(&l.target.0),
            navigation: Identifier(l.navigation.unwrap_or_else(|| l.target.0.to_lowercase())),
            associativity: l.associativity,
        }
    }
}

#[derive(Debug)]
struct ModelAssociation(ModelNavLink, ModelNavLink);

impl From<Association<'_>> for ModelAssociation {
    fn from(a: Association) -> Self {
        let Association(link1, link2) = a;
        ModelAssociation(link1.into(), link2.into())
    }
}

#[derive(Debug)]
struct Model {
    classes: HashMap<Identifier, ModelClass>,
    relationships: Vec<Rc<ModelAssociation>>,
}

impl Model {
    fn to_schema(&self) -> String {
        let mut sql = SQLBuilder::default();

        for (name, class) in &self.classes {
            let t = sql.class_table(name.clone());
            t.push((Identifier("id".to_string()), SQLType::Integer));
            for (field_name, field_type) in &class.primitive_fields {
                t.push((
                    field_name.clone(),
                    match field_type {
                        Primitive::Boolean => SQLType::Integer,
                        Primitive::Real => SQLType::Real,
                        Primitive::Integer => SQLType::Integer,
                        Primitive::String => SQLType::Text,
                    },
                ));
            }
        }

        for assoc in &self.relationships {
            let ModelAssociation(link1, link2) = &**assoc;
            match (link1.associativity, link2.associativity) {
                (Associativity::One, Associativity::Many)
                | (Associativity::One, Associativity::Optional) => {
                    sql.class_table(link2.target.clone())
                        .push((link1.navigation.clone(), SQLType::Integer));
                    // TOOD: Mark as a foreign key
                }
                (Associativity::One, Associativity::One) => {
                    sql.class_table(link2.target.clone())
                        .push((link1.navigation.clone(), SQLType::Integer));
                    sql.class_table(link1.target.clone())
                        .push((link2.navigation.clone(), SQLType::Integer));
                }
                (Associativity::Many, Associativity::Optional)
                | (Associativity::Optional, Associativity::Many)
                | (Associativity::Optional, Associativity::Optional)
                | (Associativity::Many, Associativity::Many) => {
                    let tbl = vec![
                        (link1.target.clone(), SQLType::Integer),
                        (link2.target.clone(), SQLType::Integer),
                    ];
                    sql.many_to_many_tables.push(tbl)
                }
                _ => unreachable!(), // Association objects are sorted upon creation so One links go
                                     // on link1
            }
        }

        sql.to_sqlite3()
    }
}

enum SQLType {
    Integer,
    Real,
    Text,
}

// TODO: Foreign keys etc.
type Table = Vec<(Identifier, SQLType)>;

#[derive(Default)]
struct SQLBuilder {
    class_tables: HashMap<Identifier, Table>,
    many_to_many_tables: Vec<Table>,
}

impl SQLBuilder {
    fn class_table(&mut self, name: Identifier) -> &mut Table {
        self.class_tables.entry(name).or_insert_with(Vec::new)
    }

    fn to_sqlite3(&self) -> String {
        let mut s = String::new();

        for (name, tbl) in &self.class_tables {
            s += &make_table(name, tbl);
        }

        for tbl in &self.many_to_many_tables {
            let name = Identifier(tbl[0].0 .0.clone() + &tbl[1].0 .0);
            s += &make_table(&name, tbl);
        }

        fn make_table(name: &Identifier, tbl: &Table) -> String {
            let mut s = String::new();
            s += &format!("CREATE TABLE {}", name.escape());
            if tbl.len() > 0 {
                s += " (\n";
                s += &tbl
                    .iter()
                    .map(|(column, typ)| {
                        format!(
                            "\t{} {}",
                            column.escape(),
                            match typ {
                                SQLType::Integer => "INTEGER",
                                SQLType::Real => "REAL",
                                SQLType::Text => "TEXT",
                            }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",\n");
                s += "\n)"
            }
            s += ";\n";
            s
        }

        s
    }
}

type TableAlias = Identifier;
type TableName = Identifier;
type ColumnName = Identifier;

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
    ContextVariable(String),
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Expr::SubQuery(()) => todo!(),
            Expr::Bool(cond) => write!(f, "{cond}"),
            Expr::Field(table, column) => write!(f, "{}.{}", table.escape(), column.escape()),
            Expr::Constant(c) => todo!(),
            Expr::ContextVariable(name) => write!(f, "${}", name),
        }
    }
}

enum BoolCondition {
    And(Box<BoolCondition>, Box<BoolCondition>),
    Equal(Expr, Expr),
    Gt(Expr, Expr),
}

impl std::fmt::Display for BoolCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            BoolCondition::And(a, b) => write!(f, "{a} AND {b}"),
            BoolCondition::Equal(a, b) => write!(f, "{a} = {b}"),
            BoolCondition::Gt(a, b) => write!(f, "{a} > {b}"),
        }
    }
}

// TODO: Just stuff these all in the Where clause?
type Join = ((TableAlias, ColumnName), (TableAlias, ColumnName));

// TODO: Typestate this so a base_table and output are always added

#[derive(Default)]
struct SQLQueryBuilder {
    base_table: Option<TableAlias>,
    alias_n: HashMap<TableName, usize>,
    tables: HashMap<TableAlias, TableName>,
    joins: Vec<Join>,
    r#where: Option<BoolCondition>,
    output: Option<Output>,
}

struct SQLQuery {
    base_table: TableAlias,
    joined_tables: Vec<Join>,
    tables: HashMap<TableAlias, TableName>,
    r#where: Option<BoolCondition>,
    output: Output,
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
    let doctors = sql.add_joined_table((s.clone(), "hospital".into()), ("Doctor_class".into(), "hospital".into()));
    sql.add_where(BoolCondition::Equal(
        Expr::Field(s.clone(), "id".into()),
        Expr::ContextVariable("self".to_string()),
    ));
    sql.output = Some(Output::Count(doctors.clone(), "id".into()));
    println!("hand built: {}", sql.to_sqlite3());
}

enum Type<'a> {
    Boolean,
    Integer,
    Real,
    String,
    Class(ClassName<'a>),
}

#[derive(Default)]
struct ModelBuilder<'a> {
    class_names: HashSet<ClassName<'a>>,
    classes: HashMap<ClassName<'a>, PrimitiveClassDef<'a>>,
    relationships: Vec<Association<'a>>,
}

impl<'c> ModelBuilder<'c> {
    fn build<F>(f: F) -> Model
    where
        for<'brand> F: FnOnce(&mut ModelBuilder<'brand>),
    {
        let mut builder = ModelBuilder::default();
        f(&mut builder);

        assert_eq!(
            builder.classes.len(),
            builder.class_names.len(),
            "Classes declared without being defined!"
        );
        for name in builder.class_names {
            if !builder.classes.contains_key(&name) {
                panic!("Class name was mentioned but class was never defined")
            }
        }

        let mut classes: HashMap<Identifier, ModelClass> = builder
            .classes
            .into_iter()
            .map(|(class, p)| {
                (
                    class_name(&class.0),
                    ModelClass {
                        parent: p.parent.map(|cn| class_name(&cn.0)),
                        primitive_fields: p
                            .primitive_fields
                            .into_iter()
                            .map(|(name, prim)| (Identifier(name), prim))
                            .collect(),
                        associations: HashMap::new(),
                        primitive_collections: HashMap::new(),
                    },
                )
            })
            .collect();
        let mut relationships = vec![];

        for assoc in builder.relationships {
            let assoc: Rc<ModelAssociation> = Rc::new(assoc.into());

            // Got to keep the order straight here!
            let link1 = &assoc.0;
            let link2 = &assoc.1;
            let class1 = &link2.target;
            let class2 = &link1.target;

            if classes[class1]
                .primitive_fields
                .contains_key(&link1.navigation)
            {
                panic!(
                    "Navigation named {:?} conflicts with field",
                    link1.navigation
                );
            }

            if classes[class2]
                .primitive_fields
                .contains_key(&link2.navigation)
            {
                panic!(
                    "Navigation named {:?} conflicts with field",
                    link1.navigation
                );
            }

            assert!(
                classes
                    .get_mut(class1)
                    .unwrap()
                    .associations
                    .insert(
                        link1.navigation.clone(),
                        AssocEnd {
                            association: Rc::clone(&assoc),
                            which: false
                        }
                    )
                    .is_none(),
                "Tried to insert the same navigation twice!"
            );
            assert!(
                classes
                    .get_mut(class2)
                    .unwrap()
                    .associations
                    .insert(
                        link2.navigation.clone(),
                        AssocEnd {
                            association: Rc::clone(&assoc),
                            which: true
                        }
                    )
                    .is_none(),
                "Tried to insert the same navigation twice!"
            );

            relationships.push(assoc);
        }

        Model {
            classes,
            relationships,
        }
    }

    fn class(&mut self, s: &str) -> ClassName<'c> {
        let wrapped = ClassName(s.to_string(), PhantomData);
        self.class_names.insert(wrapped.clone());
        wrapped
    }

    fn def_subclass<F>(&mut self, parent: ClassName<'c>, name: &str, f: F) -> ClassName<'c>
    where
        F: FnOnce(&mut ClassBuilder),
    {
        let name = self.def_class(name, f);
        self.classes.get_mut(&name).unwrap().parent = Some(parent);
        name
    }

    fn def_class<F>(&mut self, name: &str, f: F) -> ClassName<'c>
    where
        F: FnOnce(&mut ClassBuilder),
    {
        let name = self.class(name);
        let mut class_builder = ClassBuilder {
            name: name.clone(),
            model_builder: self,
            class: PrimitiveClassDef::default(),
        };
        f(&mut class_builder);
        let class = class_builder.class;
        self.classes.insert(name.clone(), class);
        name
    }

    fn add_relationship(
        &mut self,
        navigation1: &str,
        assoc1: Associativity,
        target1: ClassName<'c>,
        navigation2: &str,
        assoc2: Associativity,
        target2: ClassName<'c>,
    ) {
        self.relationships.push(Association::new(
            NavLink {
                target: target1,
                navigation: navigation1.to_string().into(),
                associativity: assoc1,
            },
            NavLink {
                target: target2,
                navigation: navigation2.to_string().into(),
                associativity: assoc2,
            },
        ));
    }

    fn make_link<F>(&mut self, class1: &str, class2: &str, f: F)
    where
        F: FnOnce(&mut AssocBuilder),
    {
        let class1 = self.class(class1);
        let class2 = self.class(class2);
        let mut b = AssocBuilder::new(class1, class2);
        f(&mut b);
        self.relationships.push(b.into());
    }
}

struct AssocBuilder<'brand>(NavLink<'brand>, NavLink<'brand>);

impl<'brand> AssocBuilder<'brand> {
    fn new(class1: ClassName<'brand>, class2: ClassName<'brand>) -> Self {
        AssocBuilder(
            NavLink {
                target: class1,
                navigation: None,
                associativity: Associativity::One,
            },
            NavLink {
                target: class2,
                navigation: None,
                associativity: Associativity::One,
            },
        )
    }

    fn into(self) -> Association<'brand> {
        Association::new(self.0, self.1)
    }
}

struct ClassBuilder<'a, 'brand> {
    name: ClassName<'brand>,
    model_builder: &'a mut ModelBuilder<'brand>,
    class: PrimitiveClassDef<'brand>,
}

impl<'a, 'brand> ClassBuilder<'a, 'brand> {
    fn add_field(&mut self, name: &str, typ: Primitive) -> &mut ClassBuilder<'a, 'brand> {
        assert!(
            self.class
                .primitive_fields
                .insert(name.to_string(), typ)
                .is_none(),
            "Tried to add the same field twice!"
        );
        self
    }
}

/*
 * class Hospital {
 *      name: String,
 *      doctors: *Doctor,
 *      patients: *Patient,
 * }
 *
 *  class Person {
 *      name: String,
 *      age: Integer,
 *  }
 *
 * class Patient is Person {
 *      doctor: Doctor,
 *      emergency_contact: Person
 * }
 *
 * class Doctor is Person {
 *      patients: *Patient
 * }
 *
 *
 * implicitly doctor and patient have a navigation link named "hospital" with associativity 1a
 *
 * Try to write more explicitly?
 *
 * class Hospital {
 *      name: String
 * }
 *
 * class Person {
 *      name: String,
 *      age: Integer,
 * }
 *
 *  enum Colour {
 *      Green,
 *      Red,
 *      Blue
 *  }
 *
 *  class Doctor is Person
 *  class Patient is Person
 *  class Room
 *  class Kit {
 *      gown_colour: Colour,
 *      mess_allowance: Real // Yes I know using floats for money is bad
 *  }
 *
 * Hospital has doctors: *Doctor, Doctor has Hospital
 * Hospital has patients: *Patient, Patient has hospital: Hospital
 * Doctor has patients: *Patient, Patient has doctor: Doctor
 * Patient has emergency_contact: Person, Person has dependents: *Patient
 * Patient has Kit // Implies that Kit has patient: Patient
 * Patient has Room, Room has ?Patient
 *
 * SQL
 *
 * table Hospital_class (int id, varchar name);
 * table Person_class (int id, varchar name, int age);
 *
 * table Doctor_class (foreign key int id); // References the Person superclass
 * table Patient_class (foreign key int id);
 * table Room_class (int id);
 * table Kit_class (int id, foreign key int gown_colour, real mess_allowance);
 *
 * add hospital field to table Doctor_class
 * add hospital field to table Patient_class
 * add doctor field to table Patient_class
 * add emergency_contact field to table Patient_class
 * concat Kit and Patient tables
 * add room field to table Patient_class
 *
 * LINTS
 *
 * class with no primitive fields and a single implicitly-named relationship -> Maybe meant to
 * point to a different class?
 *
 * Class A has 1 implicitly named and 1 explicitly named relationship to the same Class B -> Should
 * these relationships be merged?
 *
 * TODO
 *
 * if 2 classes have exactly one 1-1 relationship, store them in the same table
 */

#[derive(Debug)]
enum OclBool {
    Equals(OclNode, OclNode),
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

#[derive(Debug)]
enum OclNode {
    Object(TableName, String),
    AllInstances(TableName),
    Navigate(Box<OclNode>, Identifier),
    Select(Box<OclNode>, Box<OclBool>),
    Count(Box<OclNode>),
    Bool(Box<OclBool>),
}

struct Query;

fn build_query(ocl: &OclNode, model: &Model, sql: &mut SQLQueryBuilder) -> TableAlias {
    match ocl {
        OclNode::Object(tbl, object_name) => {
            let alias = sql.add_base_table(tbl.clone());
            sql.add_where(BoolCondition::Equal(
                Expr::Field(alias.clone(), "id".into()),
                Expr::ContextVariable(object_name.clone()),
            ));
            alias
        }
        OclNode::AllInstances(tbl) => {
            let alias = sql.add_base_table(tbl.clone());
            alias
        }
        OclNode::Select(node, condition) => {
            let output_tbl = build_query(node, model, sql);
            todo!()
        }
        OclNode::Count(node) => {
            let output_tbl = build_query(node, model, sql);
            sql.output = Some(Output::Count(output_tbl, "id".into()));
            "<w t f>".into()
        }
        OclNode::Navigate(node, navigation) => {
            let prev_alias = build_query(node, model, sql);
            let assoc = &model.classes[&sql.tables[&prev_alias]].associations[navigation];
            match assoc.associativity {
                Associativity::One => {
                    sql.add_joined_table((prev_alias, navigation.clone()), (assoc.target.clone(), "id".into()))
                }
                Associativity::Many => {
                    let rev = assoc.reverse();
                    match rev.associativity {
                        Associativity::One => sql.add_joined_table((prev_alias, "id".into()), (assoc.target.clone(), rev.navigation.clone())),
                        x => todo!("{:?}", x)
                    }
                }
                _ => todo!()
            }
                
                //add table
                //add join to prev_alias
                //if it's a many-many then add additional target table and join
        }
        OclNode::Bool(_) => todo!(),
    }
}

fn main() {
    let model = ModelBuilder::build(|mut builder| {
        let hospital = builder.def_class("Hospital", |class| {
            class.add_field("name", Primitive::String);
        });

        let person = builder.def_class("Person", |class| {
            class.add_field("name", Primitive::String);
            class.add_field("age", Primitive::Integer);
        });

        // TODO: Inheritance
        let doctor = builder.def_subclass(person.clone(), "Doctor", |class| {});
        let surgery = builder.def_class("Surgery Record", |class| {
            class.add_field("mortality", Primitive::Boolean);
        });
        let patient = builder.def_subclass(person.clone(), "Patient", |class| {});

        builder.add_relationship(
            "doctors",
            Associativity::Many,
            doctor.clone(),
            "hospital",
            Associativity::One,
            hospital.clone(),
        );

        builder.add_relationship(
            "patients",
            Associativity::Many,
            patient.clone(),
            "hospital",
            Associativity::One,
            hospital,
        );

        builder.add_relationship(
            "emergency_contact",
            Associativity::One,
            person,
            "dependents",
            Associativity::Many,
            patient,
        );

        builder.add_relationship(
            "surgeries",
            Associativity::Many,
            surgery,
            "doctors involved",
            Associativity::Many,
            doctor,
        );
    });

    dbg!(&model);
    println!("{}", model.to_schema());

    println!("OCL text: \ncontext self: <some external value>\nself.hospitals.doctors->size()");

    let ocl = OclNode::Count(OclNode::Navigate(OclNode::Navigate(OclNode::Object("Doctor_class".into(), "self".into()).into(), "hospital".into()).into(), "doctors".into()).into());
    println!("OCL tree: {ocl:?}");

    let mut sql = SQLQueryBuilder::default();

    build_query(&ocl, &model, &mut sql);
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
        JOIN person_class ON person_class.rowid = doctors.rowid
        WHERE self.rowid = $id
            AND doctors.age < self.age

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
