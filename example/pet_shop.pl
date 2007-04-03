use Data::Dumper;
use PetShop;

my $dbname = 'petshop.db';

unlink $dbname;
my $schema = PetShop->connect("dbi:SQLite:dbname=$dbname",'','');
$schema->deploy;

my $cat = $schema->resultset('Cat')->find_or_create({ name => 'Fluffy' });

print $cat->name, "\n";

print "Field info: ",Dumper( $cat->result_source->field_info('jim') ), "\n";

print Dumper( [ $cat->index_fields ] );

print Dumper( $cat->_fields );
