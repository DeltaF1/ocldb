use crate::Associativity;
use crate::ClassName;
use crate::ColumnName;
use crate::FieldName;
use crate::OclType;
use crate::Primitive;
use crate::TableName;
use std::cmp::min;
use std::rc::Rc;
use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
};

use pest::iterators::Pairs;
use pest::{Parser, RuleType};
use pest_derive::Parser;

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct DeclaredClassName<'a>(String, PhantomData<&'a mut ()>);

impl DeclaredClassName<'_> {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct DeclaredEnumName<'a>(String, PhantomData<&'a mut ()>);

/*enum Type<'a> {
    Boolean,
    Integer,
    Real,
    String,
    Enum(DeclaredEnumName<'a>),
    Class(DeclaredClassName<'a>),
}*/

enum InProgressPrimitive<'a> {
    Boolean,
    Integer,
    Real,
    String,
    Enum(DeclaredEnumName<'a>),
}

struct InProgressNavLink<'a> {
    target: DeclaredClassName<'a>,
    navigation: Option<String>,
    associativity: Associativity,
}

struct InProgressAssociation<'a>(InProgressNavLink<'a>, InProgressNavLink<'a>);

impl InProgressAssociation<'_> {
    fn new<'a>(
        link1: InProgressNavLink<'a>,
        link2: InProgressNavLink<'a>,
    ) -> InProgressAssociation<'a> {
        // Sort the order of the links for nicer pattern matching later
        let (link1, link2) = match (link1.associativity, link2.associativity) {
            (Associativity::One, Associativity::One) => (link1, link2),
            (_, Associativity::One) => (link2, link1),
            (Associativity::Many, _) => (link2, link1),
            _ => (link1, link2),
        };
        InProgressAssociation(link1, link2)
    }
}

#[derive(Default)]
struct PrimitiveClassDef<'brand> {
    parent: Option<DeclaredClassName<'brand>>,
    primitive_fields: HashMap<String, InProgressPrimitive<'brand>>,
}

#[derive(Debug)]
pub struct AssocEnd {
    association: Rc<Association>,
    which: bool,
}

impl std::ops::Deref for AssocEnd {
    type Target = NavLink;

    fn deref(&self) -> &NavLink {
        if self.which {
            &self.association.1
        } else {
            &self.association.0
        }
    }
}

impl AssocEnd {
    pub fn reverse(&self) -> &NavLink {
        if self.which {
            &self.association.0
        } else {
            &self.association.1
        }
    }

    fn get_raw(&self) -> &Association {
        &*self.association
    }
}

#[derive(Debug)]
pub struct Class {
    pub parent: Option<ClassName>,
    pub primitive_fields: HashMap<FieldName, Primitive>,
    pub primitive_collections: HashMap<FieldName, (Primitive, Associativity)>,
    pub associations: HashMap<FieldName, AssocEnd>,
}

#[derive(Debug)]
pub struct NavLink {
    pub target: ClassName,
    pub navigation: FieldName,
    pub associativity: Associativity,
}

impl From<InProgressNavLink<'_>> for NavLink {
    fn from(l: InProgressNavLink) -> Self {
        NavLink {
            target: l.target.0.as_str().into(),
            navigation: FieldName(l.navigation.unwrap_or_else(|| l.target.0.to_lowercase())),
            associativity: l.associativity,
        }
    }
}

#[derive(Debug)]
pub struct Association(NavLink, NavLink);

impl Association {
    fn is_one_to_one(&self) -> bool {
        matches!(
            (self.0.associativity, self.1.associativity),
            (Associativity::One, Associativity::One)
        )
    }
}

impl From<InProgressAssociation<'_>> for Association {
    fn from(a: InProgressAssociation) -> Self {
        let InProgressAssociation(link1, link2) = a;
        Association(link1.into(), link2.into())
    }
}

type EnumName = String;

#[derive(Debug)]
pub struct Enum {
    members: Vec<String>,
}

impl Enum {
    pub fn index_of(&self, member: &str) -> Option<usize> {
        self.members.iter().position(|s| s == member)
    }
}

#[derive(Debug)]
pub struct Model {
    pub classes: HashMap<ClassName, Class>,
    pub relationships: Vec<Rc<Association>>,
    pub enums: HashMap<EnumName, Enum>,
    pub islands: HashMap<ClassName, ClassName>,
}

pub(crate) mod canonicalize {
    use super::Association;
    use crate::{ClassName, ColumnName, FieldName, TableName};

    pub fn class_table_name(class: &ClassName) -> TableName {
        TableName::from_string(class.clone().into_inner() + "_class")
    }

    pub fn many_many_table_name(a: &Association) -> TableName {
        TableName::from_string(format!(
            "{class1}_{field1}_{class2}_{field2}",
            class1 = class_table_name(&a.0.target).as_str(),
            field1 = a.0.navigation.as_str(),
            class2 = class_table_name(&a.1.target).as_str(),
            field2 = a.1.navigation.as_str(),
        ))
    }

    pub fn column_name(class: ClassName, column: &FieldName) -> ColumnName {
        ColumnName::from_string(class.into_inner() + "_" + column.as_str())
    }
}

enum SqlAccessType {
    ThisField(ClassName, FieldName),
    OtherField(ClassName, FieldName),
}

impl Model {
    fn island_of<'a>(&'a self, name: &'a ClassName) -> &'a ClassName {
        self.islands.get(name).unwrap_or(name)
    }

    fn class_with_prim_field<'a>(
        &'a self,
        class: &'a ClassName,
        field: &FieldName,
    ) -> Option<&'a ClassName> {
        let mut cur = Some(class);

        while let Some(name) = cur {
            let class = self.classes.get(name)?;
            if class.primitive_fields.contains_key(field) {
                return Some(name);
            }
            cur = class.parent.as_ref();
        }
        None
    }

    fn class_with_navigation<'a>(
        &'a self,
        class: &'a ClassName,
        field: &FieldName,
    ) -> Option<&'a ClassName> {
        let mut cur = Some(class);

        while let Some(name) = cur {
            let class = self.classes.get(name)?;
            if class.associations.contains_key(field)
                || class.primitive_fields.contains_key(field)
                || class.primitive_collections.contains_key(field)
            {
                return Some(name);
            }
            cur = class.parent.as_ref();
        }
        None
    }

    pub(crate) fn table_of_prim_field(
        &self,
        class: &ClassName,
        field: &FieldName,
    ) -> Option<(TableName, ColumnName)> {
        let class = self.class_with_prim_field(class, field)?;

        let class_table = self.table_of(class);
        let field_ident = canonicalize::column_name(class.clone(), field);
        Some((class_table, field_ident))
    }

    pub(crate) fn table_of(&self, name: &ClassName) -> TableName {
        canonicalize::class_table_name(self.island_of(name))
    }

    pub fn is_subclass_of(&self, child: &ClassName, parent: &ClassName) -> bool {
        let mut child = &self.classes[child];
        while let Some(test) = &child.parent {
            if test == parent {
                return true;
            }
            child = &self.classes[test];
        }
        false
    }

    fn base_class<'a>(&'a self, class: &'a Class) -> Option<(&'a ClassName, &'a Class)> {
        class.parent.as_ref().and_then(|name| {
            let parent = &self.classes[name];
            self.base_class(parent).or(Some((name, parent)))
        })
    }

    pub fn field_of(&self, class: &Class, navigation: &FieldName) -> Option<OclType> {
        // TODO: Determine resolution order. Can sub-classes shadow super-classes?
        if let Some(a) = class.associations.get(navigation) {
            Some(OclType::Class(a.target.clone()))
        } else if let Some(p) = class.primitive_fields.get(navigation) {
            Some(OclType::Primitive(p.clone()))
        } else {
            class
                .parent
                .as_ref()
                .and_then(|parent| self.classes.get(parent))
                .and_then(|parent| self.field_of(parent, navigation))
        }
    }

    pub fn to_schema(&self) -> String {
        let mut sql = SQLSchemaBuilder::default();

        for (class_name, class) in &self.classes {
            let t = sql.class_table(self.table_of(class_name));

            if let Some((name, _)) = self.base_class(class) {
                // Subclass tables are 1?-1 with the base class
                t.add_constraint(Constraint::Foreign("id".into(), self.table_of(name)));
            }
            for (field_name, field_type) in &class.primitive_fields {
                t.add_column(
                    canonicalize::column_name(class_name.clone(), field_name),
                    match field_type {
                        Primitive::Boolean => SQLType::Integer,
                        Primitive::Real => SQLType::Real,
                        Primitive::Integer => SQLType::Integer,
                        Primitive::String => SQLType::Text,
                        Primitive::Enum(_) => SQLType::Integer,
                    },
                );
            }
        }

        /*
        Optional -><- Many

        Object1 has ?Object2, Object2 has *Object1
        Table Object1 (object2: Option<Object2id>)

         */

        for assoc in &self.relationships {
            // TODO: All columns should be NOT NULL, unless it's a many-optional relationship
            let Association(link1, link2) = &**assoc;
            match (link1.associativity, link2.associativity) {
                (Associativity::One, Associativity::Many)
                | (Associativity::One, Associativity::Optional)
                | (Associativity::Optional, Associativity::Many) => {
                    let class_name = self.table_of(&link2.target);
                    let column_name =
                        canonicalize::column_name(link2.target.clone(), &link1.navigation);
                    let target_name = self.table_of(&link1.target);
                    sql.class_table(class_name)
                        .add_column(column_name.clone(), SQLType::Integer)
                        .add_constraint(Constraint::Foreign(column_name, target_name));
                }
                (Associativity::One, Associativity::One) => {
                    let class1 = &link2.target;
                    let class2 = &link1.target;

                    //let class1 = canonicalize::class_table_name(&link2.target);
                    //let col1 = canonicalize::column_name(class1.clone(), &link1.navigation);
                    //let class2 = canonicalize::class_table_name(&link1.target);
                    //let col2 = canonicalize::column_name(class2.clone(), &link2.navigation);

                    if class1 == class2 {
                        panic!("1-1 Self relationship not allowed");
                    }

                    continue;

                    // No need for this when tables joined 1-1
                    //sql.class_table(joined_table_name)
                    //    .add_column(col1., typ)
                    /*
                    sql.class_table(class1.clone())
                        .add_column(col1.clone(), SQLType::Integer)
                        .add_constraint(Constraint::Foreign(col1, class2.clone()));
                    sql.class_table(class2)
                        .add_column(col2.clone(), SQLType::Integer)
                        .add_constraint(Constraint::Foreign(col2, class1));
                    */
                }
                (Associativity::Optional, Associativity::Optional)
                | (Associativity::Many, Associativity::Many) => {
                    let mut tbl = Table::named(canonicalize::many_many_table_name(assoc));

                    let class1 = self.table_of(&link2.target);
                    let class2 = self.table_of(&link1.target);

                    let col1 = canonicalize::column_name(link2.target.clone(), &link1.navigation);
                    let col2 = canonicalize::column_name(link1.target.clone(), &link2.navigation);
                    tbl.add_column(col1.clone(), SQLType::Integer);
                    tbl.add_column(col2.clone(), SQLType::Integer);
                    tbl.add_constraint(Constraint::Primary(vec![col1.clone(), col2.clone()]));
                    tbl.add_constraint(Constraint::Foreign(col1, class2));
                    tbl.add_constraint(Constraint::Foreign(col2, class1));
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

// TODO: Need to enable SQLite foreign key support
// https://www.sqlite.org/foreignkeys.html#fk_enable
// PRAGMA foreign_keys = ON;
type Columns = Vec<(ColumnName, SQLType)>;

enum Constraint {
    Primary(Vec<ColumnName>),
    Foreign(ColumnName, TableName),
}

struct Table {
    name: TableName,
    columns: Columns,
    constraints: Vec<Constraint>,
}

impl Table {
    fn named(name: TableName) -> Table {
        Table {
            name,
            columns: Default::default(),
            constraints: Default::default(),
        }
    }

    fn add_column(&mut self, name: ColumnName, typ: SQLType) -> &mut Self {
        self.columns.push((name, typ));
        self
    }

    fn add_constraint(&mut self, constraint: Constraint) -> &mut Self {
        self.constraints.push(constraint);
        self
    }
}

#[derive(Default)]
struct SQLSchemaBuilder {
    class_tables: HashMap<TableName, Table>,
    many_to_many_tables: Vec<Table>,
}

impl SQLSchemaBuilder {
    fn class_table(&mut self, name: TableName) -> &mut Table {
        self.class_tables.entry(name.clone()).or_insert_with(|| {
            let mut tbl = Table::named(name);
            tbl.add_constraint(Constraint::Primary(vec!["id".into()]));
            tbl.add_column("id".into(), SQLType::Integer);
            tbl
        })
    }

    fn to_sqlite3(&self) -> String {
        let mut s = String::new();

        for tbl in self.class_tables.values() {
            s += &make_table(tbl);
        }

        for tbl in &self.many_to_many_tables {
            // TODO: Table name added at creation time, not at SQL serialization time

            s += &make_table(tbl);
        }

        fn make_table(tbl: &Table) -> String {
            let mut s = String::new();
            s += &format!("CREATE TABLE {}", tbl.name.escape());
            let mut lines = tbl
                .columns
                .iter()
                .map(|(column, typ)| {
                    format!(
                        // FIXME: Some columns involved in Optional relationships can be Null
                        "\t{} {}",
                        column.escape(),
                        match typ {
                            SQLType::Integer => "INTEGER",
                            SQLType::Real => "REAL",
                            SQLType::Text => "TEXT",
                        }
                    )
                })
                .collect::<Vec<_>>();

            for constraint in &tbl.constraints {
                lines.push(match constraint {
                    Constraint::Foreign(column, references) => {
                        format!(
                            "FOREIGN KEY({}) REFERENCES {}(id)",
                            column.escape(),
                            references.escape()
                        )
                    }
                    Constraint::Primary(columns) => {
                        format!(
                            "PRIMARY KEY ({})",
                            columns
                                .iter()
                                .map(ColumnName::escape)
                                .collect::<Vec<_>>()
                                .join(",")
                        )
                    }
                });
            }

            if lines.len() > 0 {
                s += " (\n";
                s += &lines.join(",\n");
                s += "\n)"
            }

            s += ";\n";
            s
        }

        s
    }
}

#[derive(Default)]
pub(crate) struct ModelBuilder<'a> {
    class_names: HashSet<DeclaredClassName<'a>>,
    classes: HashMap<DeclaredClassName<'a>, PrimitiveClassDef<'a>>,
    relationships: Vec<InProgressAssociation<'a>>,
    enums: HashMap<DeclaredEnumName<'a>, Enum>,
    enum_names: HashSet<DeclaredEnumName<'a>>,
}

impl<'c> ModelBuilder<'c> {
    pub fn build<F>(f: F) -> Model
    where
        for<'brand> F: FnOnce(&mut ModelBuilder<'brand>),
    {
        let mut builder = ModelBuilder::default();
        f(&mut builder);

        // Because branded `ClassName`s can only be constructed by `class` which adds them to the class_names set,
        // it is impossible for a class to be defined which is not in that set. This means we only need to check
        // the inverse (that all declared classes have a definition)

        assert_eq!(
            builder.classes.len(),
            builder.class_names.len(),
            "Classes declared without being defined!"
        );

        assert_eq!(builder.enums.len(), builder.enum_names.len());

        for name in builder.class_names {
            if !builder.classes.contains_key(&name) {
                panic!("Class name was mentioned but class was never defined")
            }
        }

        let enums = builder
            .enums
            .into_iter()
            .map(|(name, e)| (name.0, e))
            .collect();

        let mut islands: HashMap<ClassName, ClassName> = Default::default();

        let mut classes: HashMap<ClassName, Class> = builder
            .classes
            .into_iter()
            .map(|(class, p)| {
                (
                    class.0.as_str().into(),
                    Class {
                        parent: p.parent.map(|cn| cn.0.as_str().into()),
                        primitive_fields: p
                            .primitive_fields
                            .into_iter()
                            .map(|(name, prim)| {
                                (
                                    FieldName(name),
                                    match prim {
                                        InProgressPrimitive::Boolean => Primitive::Boolean,
                                        InProgressPrimitive::Integer => Primitive::Integer,
                                        InProgressPrimitive::Real => Primitive::Real,
                                        InProgressPrimitive::String => Primitive::String,
                                        InProgressPrimitive::Enum(x) => {
                                            Primitive::Enum(/*FIXME EnumName*/ x.0)
                                        }
                                    },
                                )
                            })
                            .collect(),
                        associations: HashMap::new(),
                        primitive_collections: HashMap::new(),
                    },
                )
            })
            .collect();
        let mut relationships = vec![];

        for assoc in builder.relationships {
            let assoc: Rc<Association> = Rc::new(assoc.into());

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

            if matches!(
                (link1.associativity, link2.associativity),
                (Associativity::One, Associativity::One)
            ) {
                let island_root: ClassName = min(class1.as_str(), class2.as_str()).into();
                println!("min({:?}, {:?}) = {:?}", class1, class2, island_root);

                islands
                    .entry(class1.clone())
                    .and_modify(|e: &mut ClassName| *e = min(e.clone(), island_root.clone()))
                    .or_insert(island_root.clone());

                islands
                    .entry(class2.clone())
                    .and_modify(|e: &mut ClassName| *e = min(e.clone(), island_root.clone()))
                    .or_insert(island_root.clone());
            }

            relationships.push(assoc);
        }

        for (class_name, class) in &classes {
            let mut has_1_to_1 = HashSet::new();
            for assoc in class.associations.values() {
                if assoc.get_raw().is_one_to_one() {
                    assert!(
                        !has_1_to_1.contains(&assoc.target),
                        "Can't have more than one 1-1 relationship between {:?} and {:?}",
                        class_name,
                        assoc.target
                    );
                    has_1_to_1.insert(assoc.target.clone());
                }
            }
        }

        /*
        List of relationships
        class1 <-> class3
        class2
        class3 <-> class1
        class3 <-> class4
        class4 <-> class3
        class4 <-> class10
        class5 <-> class7
        class7 <-> class5
        class10 <-> class4


        ->
        Hashmap with min() applied
        class10 -> class4
        class7 -> class5
        class5 -> class5
        class4 -> class3
        class3 -> class1
        class1 -> class1
        class2 -> NONE


        ->
         */

        // TODO: Can this be done in one pass if the keys are sorted?
        // Island grouping process
        let mut changed: bool = true;
        let keys = islands.keys().map(Clone::clone).collect::<Vec<_>>();
        while changed {
            changed = false;
            for key in &keys {
                let current_root = &islands[key];
                let next_root: &ClassName = &islands[current_root];
                dbg!(key, &current_root, &next_root);
                match Ord::cmp(next_root.as_str(), current_root.as_str()) {
                    std::cmp::Ordering::Less => {
                        *islands.get_mut(key).unwrap() = next_root.clone();
                        changed = true;
                        println!("Changed!");
                    }
                    std::cmp::Ordering::Equal => (),
                    std::cmp::Ordering::Greater => {
                        let current_root = current_root.clone();
                        *islands.get_mut(&current_root).unwrap() = current_root.clone();
                        changed = true;
                        println!("Changed!");
                    }
                }
            }
        }

        Model {
            classes,
            relationships,
            islands,
            enums,
        }
    }

    pub fn class(&mut self, s: &str) -> DeclaredClassName<'c> {
        let wrapped = DeclaredClassName(s.to_string(), PhantomData);
        self.class_names.insert(wrapped.clone());
        wrapped
    }

    pub fn def_subclass<F>(
        &mut self,
        parent: DeclaredClassName<'c>,
        name: &str,
        f: F,
    ) -> DeclaredClassName<'c>
    where
        F: FnOnce(&mut ClassBuilder),
    {
        let name = self.def_class(name, f);
        self.classes.get_mut(&name).unwrap().parent = Some(parent);
        name
    }

    pub fn def_class<F>(&mut self, name: &str, f: F) -> DeclaredClassName<'c>
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

    pub fn enum_named(&mut self, s: &str) -> DeclaredEnumName<'c> {
        let wrapped = DeclaredEnumName(s.to_string(), PhantomData);
        self.enum_names.insert(wrapped.clone());
        wrapped
    }

    pub fn def_enum(&mut self, name: &str, members: Vec<String>) {
        let name = self.enum_named(name);
        self.enums.insert(name, Enum { members });
    }

    pub fn add_relationship(
        &mut self,
        navigation1: &str,
        assoc1: Associativity,
        target1: DeclaredClassName<'c>,
        navigation2: &str,
        assoc2: Associativity,
        target2: DeclaredClassName<'c>,
    ) {
        self.relationships.push(InProgressAssociation::new(
            InProgressNavLink {
                target: target1,
                navigation: navigation1.to_string().into(),
                associativity: assoc1,
            },
            InProgressNavLink {
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

struct AssocBuilder<'brand>(InProgressNavLink<'brand>, InProgressNavLink<'brand>);

impl<'brand> AssocBuilder<'brand> {
    fn new(class1: DeclaredClassName<'brand>, class2: DeclaredClassName<'brand>) -> Self {
        AssocBuilder(
            InProgressNavLink {
                target: class1,
                navigation: None,
                associativity: Associativity::One,
            },
            InProgressNavLink {
                target: class2,
                navigation: None,
                associativity: Associativity::One,
            },
        )
    }

    fn into(self) -> InProgressAssociation<'brand> {
        InProgressAssociation::new(self.0, self.1)
    }
}

pub struct ClassBuilder<'a, 'brand> {
    name: DeclaredClassName<'brand>,
    model_builder: &'a mut ModelBuilder<'brand>,
    class: PrimitiveClassDef<'brand>,
}

impl<'a, 'brand> ClassBuilder<'a, 'brand> {
    fn add_field(
        &mut self,
        name: &str,
        typ: InProgressPrimitive<'brand>,
    ) -> &mut ClassBuilder<'a, 'brand> {
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

#[derive(Parser)]
#[grammar = "schema.pest"]
struct SchemaParser;

pub fn parse_schema(string: &str) -> Result<Model, pest::error::Error<impl RuleType>> {
    let statements = SchemaParser::parse(Rule::schema, &string)?
        .next()
        .unwrap()
        .into_inner();

    let mut implicitly_named = vec![];

    let model = ModelBuilder::build(|builder| {
        for s in statements {
            match s.as_rule() {
                Rule::classdef => {
                    let mut pairs = s.into_inner();
                    let class_name = pairs.next().unwrap().as_str();
                    let superclass = match pairs.peek().as_ref().map(pest::iterators::Pair::as_rule)
                    {
                        Some(Rule::identifier) => Some(pairs.next().unwrap().as_str()),
                        _ => None,
                    };

                    fn f(
                        fields: Option<pest::iterators::Pair<'_, Rule>>,
                        class_builder: &mut ClassBuilder,
                    ) {
                        if let Some(fields) = fields {
                            for field_def in fields.into_inner() {
                                match field_def.as_rule() {
                                    Rule::fielddef => {
                                        let mut pairs = field_def.into_inner();
                                        let name = pairs.next().unwrap().as_str();
                                        let typ = pairs.next().unwrap();
                                        assert!(matches!(typ.as_rule(), Rule::primitivetype));
                                        // TODO: primitive fields with >1 associativity
                                        let typ = typ.into_inner().as_str();
                                        let typ = match typ {
                                            "Integer" => InProgressPrimitive::Integer,
                                            "Boolean" => InProgressPrimitive::Boolean,
                                            "Real" => InProgressPrimitive::Real,
                                            "String" => InProgressPrimitive::String,
                                            x => InProgressPrimitive::Enum(
                                                class_builder.model_builder.enum_named(x),
                                            ),
                                            _ => unreachable!(),
                                        };
                                        class_builder.add_field(name, typ);
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        }
                    }
                    match superclass {
                        Some(superclass_name) => {
                            let parent = builder.class(superclass_name);
                            builder.def_subclass(parent, class_name, |c| f(pairs.next(), c))
                        }
                        None => builder.def_class(class_name, |c| f(pairs.next(), c)),
                    };
                }
                Rule::enumdef => {
                    let mut pairs = s.into_inner();
                    let enum_name = pairs.next().unwrap().as_str();
                    let list = pairs.next().unwrap().into_inner();
                    builder.def_enum(
                        enum_name,
                        list.into_iter().map(|x| x.as_str().to_string()).collect(),
                    );
                }
                Rule::relationship => {
                    struct Half<'brand> {
                        from: DeclaredClassName<'brand>,
                        by: String,
                        assoc: Associativity,
                        to: DeclaredClassName<'brand>,
                    }

                    let mut halves = s.into_inner();
                    let half1 = halves.next().unwrap().into_inner();
                    let mut parse_half = |mut half: Pairs<Rule>| -> Half {
                        let class1 = builder.class(half.next().unwrap().as_str());
                        let (nav, assoc, class2) = {
                            let nav_or_class = half.next().unwrap().as_str();
                            let (nav, assoc, class);
                            match half.next() {
                                Some(typ) => {
                                    nav = nav_or_class.to_string(); // FIXME
                                    let typ = typ.into_inner().next().unwrap();
                                    let class_str;
                                    match typ.as_rule() {
                                        Rule::manyrelationship => {
                                            class_str = typ.into_inner().next().unwrap().as_str();
                                            assoc = Associativity::Many
                                        }
                                        Rule::identifier => {
                                            class_str = typ.as_str();
                                            assoc = Associativity::One;
                                        }
                                        Rule::optionalrelationship => {
                                            class_str = typ.into_inner().next().unwrap().as_str();
                                            assoc = Associativity::Optional;
                                        }
                                        _ => unreachable!(),
                                    }
                                    class = builder.class(class_str);
                                }
                                None => {
                                    // TODO: Add to the implicit list

                                    class = builder.class(nav_or_class);
                                    nav = nav_or_class.to_lowercase();
                                    assoc = Associativity::One;
                                    implicitly_named
                                        .push((class.as_str().to_string(), nav.clone()));
                                }
                            };
                            (nav, assoc, class)
                        };
                        Half {
                            from: class1,
                            by: nav,
                            assoc: assoc,
                            to: class2,
                        }
                    };

                    let half1 = parse_half(half1);
                    let half2 = halves.next();
                    let half2 = match half2 {
                        Some(h2) => {
                            let h2 = parse_half(h2.into_inner());
                            assert_eq!(half1.from, h2.to);
                            assert_eq!(half1.to, h2.from);
                            h2
                        }
                        None => {
                            let nav_name = half1.from.clone().as_str().to_lowercase();
                            implicitly_named
                                .push((half1.from.as_str().to_string(), nav_name.clone()));
                            Half {
                                from: half1.to.clone(),
                                by: nav_name,
                                assoc: Associativity::One,
                                to: half1.from.clone(),
                            }
                        }
                    };
                    builder.add_relationship(
                        &half1.by,
                        half1.assoc,
                        half1.to,
                        &half2.by,
                        half2.assoc,
                        half2.to,
                    )
                }
                Rule::EOI => continue,
                _ => unreachable!(),
            }
        }
    });
    for (class, nav) in implicitly_named {
        let class_name = ClassName(class);
        let class = &model.classes[&class_name];

        // TODO: This actually isn't necessary because every class must also be declared
        if class.associations.len() == 1 && class.primitive_fields.is_empty() {
            println!("[LINT] Class {class_name:?} has only a single (implicitly-named) association. Is it possible this class was created via a typo?")
        }
    }
    Ok(model)
}
