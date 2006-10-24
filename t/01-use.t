use Test::More tests => 2;

BEGIN { 
    use_ok( 'DBIx::Class::Index' );
    use_ok( 'LuceneQuery::Abstract' );
}
