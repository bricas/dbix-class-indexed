package DBIx::Class::Index;

use strict;
use warnings;

use LuceneQuery::Abstract;
use Scalar::Util qw( blessed );
use Text::Unidecode qw( unidecode );
use WebService::Lucene;
use WebService::Lucene::Document;
use WebService::Lucene::Field;
use WebService::Lucene::Index;

our $VERSION = '0.01';

use base qw( DBIx::Class );

__PACKAGE__->mk_classdata( fields => {} );
__PACKAGE__->mk_classdata( index => undef );
__PACKAGE__->mk_classdata( auto_index => 0 );
__PACKAGE__->mk_classdata( index_on_insert => 1 );
__PACKAGE__->mk_classdata( index_on_update => 1 );
__PACKAGE__->mk_classdata( servers => {} );

my %FIELD_TYPES = (
    #
    # MySQL types
    #
    bigint     => 'keyword',
    double     => 'keyword',
    decimal    => 'keyword',
    float      => 'keyword',
    int        => 'keyword',
    mediumint  => 'keyword',
    smallint   => 'keyword',
    tinyint    => 'keyword',
    char       => 'text',
    varchar    => 'text',
    longtext   => 'text',
    mediumtext => 'text',
    text       => 'text',
    tinytext   => 'text',
    tinyblob   => 'text',
    blob       => 'text',
    mediumblob => 'text',
    longblob   => 'text',
    enum       => 'text',
    set        => 'text',
    date       => 'text',
    datetime   => 'text',
    time       => 'text',
    timestamp  => 'text',
    year       => 'text',

    #
    # Oracle types
    #
    number     => 'keyword',
    char       => 'text',
    varchar2   => 'text',
    long       => 'keyword',
    CLOB       => 'text',
    date       => 'text',

    #
    # Sybase types
    #
    int        => 'keyword',
    money      => 'keyword',
    varchar    => 'text',
    datetime   => 'keyword',
    text       => 'text',
    real       => 'keyword',
    comment    => 'text',
    bit        => 'keyword',
    tinyint    => 'keyword',
    float      => 'keyword',
);

=head1 NAME

DBIx::Class::Index - Automatic indexing of DBIx::Class objects via 
WebService::Lucene

=head1 SYNOPSIS

    package foo;
    
    use base qw( DBIx::Class );
    
    __PACKAGE__->load_components( qw( Index Core ) );
    __PACKAGE__->table('foo');
    __PACKAGE__->index('foo_index'); # otherwise, defaults table
    __PACKAGE__->add_columns(
        foo_id => {
            data_type => 'integer'
        },
        name => {
            data_type => 'varchar',
            size      => 10,
            index     => {
                type   => 'text',
                browse => 1,
                sort   => 1
            }
        },
        location => {
            data_type => 'varchar',
            size      => 50,
            index     => 'text'
        }
    );
    
    __PACKAGE__->has_many( widgets => 'widget' );
    __PACKAGE__->add_fields(
        widget => {
            source => 'widgets.name'
            type   => 'unstored'
            browse => 1
        },
        widget_updated => {
            source  => 'widgets.ctime.epoch',
            boolean => 'is'
        },
        author => {
            source => sub {
                map {
                    join ' ', $_->first_name, $_->last_name
                } shift->authors
            },
        }
    );
    
    
    
    ...
    
    
    
    my $schema = MySchema->connect(
        'dbi:FOO...',
        $username,
        $password,
        {
            index => {
                server => 'http://localhost:8080/lucene/'
            }
        }
    );
    
    $foo->name('This is the song that never ends...');
    $foo->update; # index will automatically be updated
    
    
    # if you're in the group that likes interacting with the index 
    # manually, all the power to you!
    $webservice->get_index('fooindx')->add_document($foo->as_document);
    
    
=head1 DESCRIPTION

This is a DBIx::Class component to make full-text indexing a seamless 
part of database objects. All that is required is the registration of
desired fields for the index. Notice that fields are not necessarily the
same as columns. For instance, suppose you have a schema representing
a film and its actors. The index representing the film table may have
a fields called 'actor' which can have multiple values, depending on
the number of actors associated with the film.
    
    package Film;
    
    __PACKAGE__->add_fields(
        actor => {
            type   => 'text',
            source => 'actors.name'
        },
    );
    

=head1 METHODS

=head2 service ( )

Retrieves an instance of WebService::Lucene representing the current
connection to the Lucene web service.

=cut

sub service {
    my $self = shift;
    
    my $parameters = eval {
        $self->result_source->schema->storage->connect_info->[3]->{index}
    };
    
    # this should probably be optimized (cached)
    if ($parameters and my $server = eval { $parameters->{server} }) {
        unless ( $self->servers->{ $server } ) {
            $self->servers->{ $server } = WebService::Lucene->new( $server );
        }
        return $self->servers->{ $server };
    }
    
    return eval { WebService::Lucene->new( 'http://localhost:8080/lucene/' ) };
}

=head2 field_type ( $column )

Attempts to determine what Lucene type (text, keyword, unstored, etc...)
corresponds to the specified SQL column type. Mappings are compatible 
with several database management systems, including MySQL, Sybase and 
Oracle, among others.

=cut

sub field_type {
    my ($self, $column) = @_;
    
    if (my $info = $self->result_source->column_info( $column )) {
        return $FIELD_TYPES{ $info->{data_type} };
    }
}

=head2 index_name ( )

Determines the name of the result set's associated index. If the name 
of its index has not been explicitly set, it defaults to the name of 
its table.

=cut

sub index_name {
    my $self = shift;
    return $self->index || $self->table;
}

=head2 get_index ( )

Attempts to retrieve the associated WebService::Lucene::Index objects 
representing this result set's associated index.

=cut

sub get_index {
    my $self = shift;
    if ( my $service = $self->service ) {
        return $service->get_index( $self->index_name );
    }
    else {
        warn "Could not acquire lucene web service";
    }
}

=head2 primary_column ( )

Retrieves the primary column of the result set. An error occurs when no 
or more than 1 primary columns exist.

=cut

sub primary_column {
    my $self = shift;
    my @primary_columns = $self->result_source->primary_columns;
    if (not @primary_columns or @primary_columns > 1) {
        die 'Indexing requires one primary column';
    }
    return $primary_columns[0];
}

=head2 as_index ( )

Constructs a WebService::Lucene::Index object suitable for use when 
creating a new index via the WebService::Lucene interface.

=cut

sub as_index {
    my $self = shift;
    my $name = $self->index_name;
    
    my $fields = $self->fields;
    return undef unless %$fields;
    
    # add the core configuration
    my $primary_column = $self->primary_column;
    my %properties = (
        'index.defaultoperator' => 'AND',
        'index.summary'         => $name,
        'index.title'           => $name,
        'document.defaultfield' => 'all',
        'document.identifier'   => $primary_column,
        'document.title'        => "[${primary_column}]",
    );
    
    # attempt to discover which column represents the updated time of a
    # document
    if ($self->can('mtime_columns')
        and
        my @mtime_columns = @{ $self->mtime_columns }
    ) {
        $properties{'document.updated'} = $mtime_columns[0];
    }
    
    # attempt to determine what field determines the title of a document
    my $title;
    for my $name (keys %$fields) {
        my $info = $fields->{$name};
        if ($info->{role} and $info->{role} eq 'title') {
            $title = $name;
            last;
        }
    }
    
    # add document title configuration
    if ($title) {
        $properties{'document.title'} = "[$title]";
    }
    
    return WebService::Lucene::Index->new({
        name       => $name,
        properties => \%properties,
    });
}

=head2 register_column ( $column, \%info )

Behaves similar to DBIx::Class register_column.
If %info contains the key 'index', calls
register_field().

=cut

sub register_column {
    my ($class, $column, $info) = @_;
    $class->next::method( $column, $info );
    
    if (exists $info->{index}) {
        $class->register_field( $column => $info->{index} );
    }
}

=head2 add_fields ( @fields )

Behaves similarly to DBIx::Class add_columns.
Calls register_field.

=cut

sub add_fields {
    my ($class, @fields) = @_;
    my $fields = $class->fields;
    
    while (my $field = shift @fields) {
        # If next entry is { ... } use that for the column info, if not
        # use an empty hashref
        my $field_info = ref $fields[0] ? shift @fields : {};
        $class->register_field( $field, $field_info );
    }
}

=head2 register_field ( $field, \%info [, \%column_info ] )

Registers a field with the result set. Optionally accepts a third 
argument corresponding to the hash references used to register an 
associated column.

=cut

sub register_field {
    my ($class, $field, $info, $column_info) = @_;
    $info = {} unless defined $info;
    my $fields = $class->fields;
    my %field  = ( name => $field );
    
    unless (ref $info) {
        $info = undef if $info =~ /^\d+$/;
        if ($column_info and my $type = $column_info->{data_type}) {
            $info ||= $FIELD_TYPES{ $type };
        }
        
        $field{type} = $info;
    }
    elsif (ref $info eq 'HASH') {
        if ($column_info and $column_info->{data_type}) {
            $info->{type} ||= $FIELD_TYPES{$column_info->{data_type}};
        }
        %field = ( %field, %$info );
    }
    
    $fields->{ $field } = \%field;
}

=head2 add_relationship ( )

=cut

sub add_relationship {
    my $self = shift;
    my ($rel, $f_source_name, $cond, $attrs) = @_;
    #$f_source_name->load_components( qw( Index ) );
    $self->next::method( @_ );
}

=head2 as_document ( [ $document ] )

If optional document is passed to method, will replaces document's
current fields with those as outlined by the registered fields of this 
result set. The optional document is not passed to method, will 
construct a new WebService::Lucene::Document object, populate it, and 
return it.

=cut

sub as_document {
    my ($self, $document) = @_;
    $document ||= WebService::Lucene::Document->new;
    
    my $fields = $self->fields;
    return undef unless %$fields;
    
    # ensure the primary column is included in the indexing
    my $primary_column = $self->primary_column;
    my @primary_fields = grep { $_ eq $primary_column } keys %$fields;
    
    # set the primary key's field type to keyword
    unless (
            exists $fields->{$primary_column}
        and exists $fields->{$primary_column}->{type}
    ) {
        $fields->{ $primary_column }->{ type } = 'keyword';
    }
    
    my @fields;
    
    # for each field...
    for my $name (keys %$fields) {
        my $info    = $fields->{$name};
        my $source  = $info->{source} || $name;
        my $type    = $info->{type} || 'text';
        my @filters = map  { ref $_ eq 'ARRAY' ? @$_ : $_ }
                      grep { $_ }
                      map  { $info->{ $_ } } qw( filter filters );
        my @values;
        
        
        
        # resolve values
        if (ref $source eq 'CODE') {
            @values = $source->( $self );
        }
        elsif (not ref $source) {
            # resolve accessors
            my @accessors = split /\./, $source;
            
            # no use calling 'me' on myself...
            shift @accessors if lc $accessors[0] eq 'me';
            
            # traverse accessors
            @values = $self;
            for my $accessor (@accessors) {
                @values = grep { defined $_ }
                          map  {
                                blessed $_ and $_->can($accessor) ? $_->$accessor
                              : ref $_ eq 'HASH'                  ? $_->{$accessor}
                              :                                     undef
                          } @values;
            }
        }
        
        # make all those fields
        for my $value (@values) {
            
            for my $filter (@filters) {
                $value = apply_filter( $value, $filter );
            }
            
            my $field = WebService::Lucene::Field->new({
                name  => $name,
                value => $value,
                %$info,
                type  => $type,
            });
            
            push @fields, $field unless $info->{exclude_from_default};
            
            $document->add($field);
        }
        
        # make all those browse fields
        if ($info->{browse}) {
            for my $value (@values) {
                $document->add(
                    WebService::Lucene::Field->new({
                        %$info,
                        name  => "browse_$name",
                        value => scalar $self->make_browse_value( $name => $value ),
                        type  => 'sorted',
                    })
                );
            }
        }
        
        # make all those sort fields
        if ($info->{sort}) {
            for my $value (@values) {
                $document->add(
                    WebService::Lucene::Field->new({
                        %$info,
                        name  => "sort_$name",
                        value => scalar $self->make_sort_value( $name => $value ),
                        type  => 'sorted',
                    })
                );
            }
        }
        
        # make the boolean values
        if (my $clause = $info->{boolean}) {
            $clause = 'has' if $clause =~ /^\d+$/;
            $document->add(
                WebService::Lucene::Field->new({
                    %$info,
                    name  => "${clause}_${name}",
                    value => @values ? 1 : 0,
                    type  => 'keyword',
                })
            );
        }
    }
    
    # add the 'all' field
    $document->add(
        WebService::Lucene::Field->new({
            name  => 'all',
            type  => 'unstored',
            value => join ' ', grep {$_} map { $_->value } @fields,
        })
    );
    
    return $document;
}

=head2 apply_filter ( $value, $filter )

Applies a particular filter to the provided value. The only filter 
currently supported is 'trim'. 

=cut

sub apply_filter {
    my ($value, $filter) = @_;
    return undef unless defined $value;
    
    if ( lc $filter eq 'trim' ) {
        $value =~ s/\s+$//;
        $value =~ s/^\s+//;
    }
    
    return $value;
}




=head2 update_dependencies ( )

Automatically updates those result set's whose indexing strategy depends
upon content found within this result set.

=cut

sub update_dependencies {
    my $self = shift;
}

=head2 insert ( )

Calls update_or_add_document(), then calls the super class's insert() 
method.

=cut

sub insert {
    my $self   = shift;
    my $result = $self->next::method( @_ );
    
    if ($self->index_on_insert or $self->auto_index) {
        $self->update_or_add_document;
        $self->update_dependencies;

        if( $self->is_changed ) {
            $result = $self->next::method( @_ );
        }
    }
    
    return $result;
}

=head2 update ( )

Calls update_or_add_document(), then calls the super class's update() 
method.

=cut

sub update {
    my $self   = shift;
    my $result = $self->next::method( @_ );
    
    if ($self->index_on_update or $self->auto_index) {
        $self->update_or_add_document;
        $self->update_dependencies;

        if( $self->is_changed ) {
            $result = $self->next::method( @_ );
        }
    }

    return $result;
}

=head2 delete ( )

Deletes document from associated index, then calls the super class's 
delete() method.

=cut

sub delete {
    my $self = shift;
    
    if ($self->auto_index) {
        # attempt to retrieve the index
        my $index = $self->get_index;
        if (not $index and my $service = $self->service) {
            $index = $service->create_index( $self->as_index );
        }
        
        unless ($index) {
            my $warn = 'no index specified';
            $warn .= '; database and index may no longer be synchronized';
            warn $warn;
            return;
        }
        
        if (my $document = eval { $index->get_document( $self->id ) }) {
            $document->delete;
        }
        
        $self->update_dependencies;
    }
    
    $self->next::method( @_ );
}

=head2 update_or_add_document ( )

Will either update or add a document to the associated index, depending 
upon whether or not it already exists within the index.

=cut

sub update_or_add_document {
    my $self = shift;
    
    my $fields = $self->fields;
    return undef unless keys %$fields;
    
    # attempt to retrieve the index
    my $index = $self->get_index;
    if (not $index and my $service = $self->service) {
        $index = $service->create_index( $self->as_index );
    }
    
    unless ( blessed $index ) {
        die "no index found for '" . $self->index_name . "'; database and index may no longer be synchronized";
    }
    
    if (my $document = eval { $index->get_document( $self->id ) }) {
        $document->clear_fields;
        $self->as_document( $document );
        $document->update;
    }
    else {
        $index->add_document( $self->as_document );
    }
}





=head2 remove_stop_words ( )

Produces a copy of the supplied string void of all applicable stop 
words.

=cut

sub remove_stop_words {
    my $s = shift;
    
    return $s unless defined $s;
    
    my @stop_words = ('The\b', '\bA\b', '\bAn\b', '\bAu\b', '\bAux\b',
        '\bLa\b', '\bLe\b', '\b'."L'", '\bLes\b', '\bDes\b', '\bUn\b',
        '\bUne\b', '\b'."D'", '\bDu\b', '\bDe la\b', '\bDe\b');
    
    $s =~ s/$_//i foreach @stop_words;
    
    $s =~ s/^\s+//;
    $s =~ s/\s+$//;
    
    return $s;
}

=head2 make_browse_value ( $scalar )

Produces a browsable string conforming to the NALD indexing strategy.

=cut

sub make_browse_value {
    my ($self, $name, $value) = @_;
    #return make_sort_value( @_ );
    return $value;
}

=head2 make_sort_value ( $scalar )

Produces a sortable string conforming to the NALD indexing strategy.

=cut

sub make_sort_value {
    my ($self, $name, $value) = @_;
    return undef unless defined $value;
    
    # Transliterate
    $value = unidecode($value);
    
    # Remove stop words
    $value = lc remove_stop_words($value);
    
    # Trim non-alphanumeric characters from start
    $value =~ s/^[^\dA-Z]+//gsi;
    
    # Prepend a '1' if the string begins with a non-letter
    $value = "1$value" if $value =~ /^\W/ || $value =~ /^\d/;
    
    return $value;
}







=head2 search_index ( [ \%cond [, \%attr ] ] ) ** EXPERIMENTAL **

Allows for searching of the index and automatic inflation of returned
results.

=cut

sub search_index {
    my ($self, $cond, $attr) = @_;
    $attr ||= {};
    
    my $lucene_query_syntax = LuceneQuery::Abstract->new;
    $attr->{searchTerms} ||= $lucene_query_syntax->where( $cond );
    $attr->{startPage} ||= $attr->{page};
    $attr->{startPage} ||= 1;
    $attr->{count} ||= $attr->{rows};
    $attr->{count} ||= 10;
    
    my $index = $self->get_index;
    
    my $results = $index->search( $attr->{searchTerms}, $attr );
    
    my $primary_column = $self->primary_column;
    
    my @documents   = $results->documents;
    my @identifiers = map { $_->$primary_column } @documents;
    my @relevances  = map { $_->relevance } @documents;
    
    my %objects = map { $_->id => $_ } $self->search(
        {
            $primary_column => \@identifiers
        }
    );
    my @objects = map { $objects{$_} } @identifiers;
    
    my $i = 0;
    for my $relevance (@relevances) {
        if (my $object = $objects[$i]) {
            $object->{relevance} = $relevance;
        }
        $i++;
    }
    
    return wantarray ? @objects : \@objects;
}


=head2 isearch ( [ \%cond [, \%attr ] ] ) ** EXPERIMENTAL **

An alias for search_index.

=cut

*isearch = \&search_index;



=head1 SEE ALSO

=over 4

=item * DBIx::Class

=item * WebService::Lucene

=item * The Lucene Web Service (http://www.lucene-ws.net/)

=back

=head1 AUTHOR

=over 4

=item * Adam Paynter E<lt>adapay@cpan.orgE<gt>

=back

=head1 COPYRIGHT AND LICENSE

Copyright 2006 by Adam Paynter

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut


1;
