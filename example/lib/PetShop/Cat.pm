package PetShop::Cat;

use strict;
use warnings;
use base qw( DBIx::Class );

__PACKAGE__->load_components( qw( Indexed Core ) );
__PACKAGE__->set_indexer(
    'WebService::Lucene',
    {
        server => 'http://localhost:8080/lucene/',
        index  => 'petshop',
    },
);
__PACKAGE__->table('cat');
__PACKAGE__->add_columns(
    cat_id => {
        data_type         => 'integer',
        is_auto_increment => 1,
    },
    name => {
        data_type => 'varchar',
        size      => 512,
        indexed   => 1,
    },
    color => {
        data_type   => 'varchar',
        size        => 100,
        is_nullable => 1,
        indexed     => 1,
    },
    age => {
        data_type   => 'integer',
        indexed     => 1,
        is_nullable => 1,
    },
);
__PACKAGE__->set_primary_key( qw( cat_id ) );

1;
