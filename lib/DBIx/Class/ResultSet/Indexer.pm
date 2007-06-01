package DBIx::Class::ResultSet::Indexer;

use strict;
use warnings;

use base qw( DBIx::Class::ResultSet );

=head1 NAME

DBIx::Class::ResultSet::Indexer - Base class for searching indices.

=head1 SYNOPSIS

    package Foo;
    
    use base qw( DBIx::Class );
    
    __PACKAGE__->load_components( qw( Indexed Core ) );
    __PACKAGE__->set_indexer( 'Bar' );
    __PACKAGE__->resultset_class( 'DBIx::Class::ResultSet::Indexer::Bar' );
    
    # elsewhere ...
    
    my $rs = $schema->resultset( 'Foo' )->search_index( $query );

=head1 METHODS

=head2 search_index( )

Searches the index with a given query.

=cut

sub search_index {
}

=head2 count_index( )

Searches the index with a given query, and returns the number of results.

=cut

sub count_index {
}

=head1 AUTHOR

=over 4

=item * Brian Cassidy E<lt>bricas@cpan.orgE<gt>

=back

=head1 COPYRIGHT AND LICENSE

Copyright 2007 by Brian Cassidy

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut

1;
