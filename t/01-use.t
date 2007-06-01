use Test::More tests => 3;

BEGIN { 
    use_ok( 'DBIx::Class::Indexed' );
    use_ok( 'DBIx::Class::Indexer' );
    use_ok( 'DBIx::Class::ResultSet::Indexer' );
}
