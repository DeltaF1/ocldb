use crate::sql_tree::*;

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
                                    Expr::Field("doctors1".into(), ColumnSpec::Named("id".into())),
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
                            Expr::Field("surgery_assoc".into(), ColumnSpec::Named("doctor".into())),
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
                                    Expr::Field("doctors1".into(), ColumnSpec::Named("id".into())),
                                    Expr::Field("doctors2".into(), ColumnSpec::Named("id".into())),
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
