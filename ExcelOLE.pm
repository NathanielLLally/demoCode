#  
#  handle auto abs path of file names
#  and facilitate easily going back and forth to base name 
#
package fileName;
use Moose;
use Moose::Util::TypeConstraints;
use File::Basename;
use File::Spec;

subtype 'baseFileNameNoExt'
  => as 'Str'
  => where  { $_ !~ /[\\\/\.]/ };

coerce 'baseFileNameNoExt'
  => from 'Str'
  => via {
    (basename($_) =~ /^(.*?)(\..*?)?$/ && $1);
  };

subtype 'baseFileName'
  => as 'Str'
  => where  { $_ !~ /[\\\/]/ };

coerce 'baseFileName'
  => from 'Str'
  => via {
    basename($_);
  };

subtype 'absDirName'
  => as 'Str'
  => where { $_ =~ /[\\\/]/ && $_ !~ /\./ };

coerce 'absDirName'
  => from 'Str'
  => via {
    File::Spec->rel2abs(dirname($_));
  };

has 'baseNoExt' => (isa => 'baseFileNameNoExt', is => 'rw', coerce => 1, lazy => 1, default => sub { '' } );
  
has 'base' => (isa => 'baseFileName', is => 'rw', coerce => 1, lazy => 1, default => sub { '' } );
  
has 'name' => (isa => 'Str', is => 'rw',
  predicate => 'hasFileName', lazy => 1, default => sub { 'Book1.xls'; },
  reader => 'get_name',
  writer => 'set_name',
);

has 'dir' => (isa => 'absDirName', is => 'rw', coerce => 1, default => sub { '.' });

sub name {
  my $s = shift;

  return $s->get_name()
    unless @_;

  $s->baseNoExt(@_);
  $s->base(@_);
  $s->dir(@_);

  $s->set_name($s->dir . "\\" . $s->base);

  return $s->dir . "\\" . $s->base;
};

around BUILDARGS => sub {
  my $orig = shift;
  my $class = shift;

  if ( @_ == 1 && ! ref $_[0] ) {
    return $class->$orig(name => $_[0]);
  }
  else {
    return $class->$orig(@_);
  }
};

sub BUILD {
  my $s = shift;
  if ($s->hasFileName) {
    $s->name($s->get_name);
  }
  $s;
}

no Moose;
__PACKAGE__->meta->make_immutable;

####################################################################
#
#  this is the object that the rest of these classes will have
#
#  has a, has A, HAS A !
#
####################################################################
#  Win32::OLE objects cache information about themselves,
#    therefore, true subclassing is messy beyong the worth
#    over auto-loaded delagation
#
package Win32::OLE::Excel;
use Win32::OLE qw(in with);
use Win32::OLE::Const 'Microsoft Excel';
use Win32::OLE::Variant;
use Win32::OLE::NLS qw(:LOCALE :DATE);
use Try::Tiny;
use Cwd;

use Moose;
use vars qw/$AUTOLOAD/;

has 'loglvl' => (
  isa => 'Num',
  is => 'rw',
  default => 0
);

sub DEBUG
{
  my ($s, $msg, @fmt) = @_;

  if ($s->loglvl > 3) {
    printf ($msg."\n", @fmt);
  }
}

#use base 'Win32::OLE';
has 'ex' => (
  isa => 'Win32::OLE', 
  is => 'rw',
#
#  consider this the constructor, run as object instantiated
#    otherwise, it will fail to instantiate excel
#
#  lazy => 1,
  default => sub {
    my $ex = Win32::OLE->GetActiveObject('Excel.Application');
    if (not defined $ex) {
      $ex = Win32::OLE->new('Excel.Application', 'Quit');
    }
    $ex->{DisplayAlerts}=0;
    $ex->{SheetsInNewWorkbook} = 1;
#    $ex->{Visible} = 0 ;

    $Win32::OLE::Warn = 3; # Die on Errors.
    $ex;
  },
);

sub DEMOLISH
{
  my $s = shift;
  try {
#    $s->ex->Quit;
  };
}

sub AUTOLOAD {
  my $s = shift;
  $AUTOLOAD =~ s/^.*:://;
  $s->DEBUG("AUTOLOAD: calling %s(%s)", $AUTOLOAD, join(",", @_));
  $s->ex->$AUTOLOAD(@_);
}

around 'BUILDARGS' => sub {
 my $orig = shift;
 my $class = shift;

 if ( @_ == 1 && ref $_[0] ) {
   return $class->$orig(ex => $_[0]);
 }
 else {
   return $class->$orig(@_);
 }
};

no Moose;
__PACKAGE__->meta->make_immutable;

#
# is-a
#
package Sheet;

use Try::Tiny;
use Moose;
use Moose::Util::TypeConstraints;
use Win32::OLE qw(in with);
use Win32::OLE::Const 'Microsoft Excel';
use Win32::OLE::Variant;
use Win32::OLE::NLS qw(:LOCALE :DATE);
use Carp qw/croak confess carp/;
use Data::Dumper;

extends 'Win32::OLE::Excel';

around 'BUILDARGS' => sub {
  my $orig = shift;
  my $class = shift;

  if ( @_ == 1 && ref $_[0] ) {
    return $class->$orig(ex => $_[0]);
  }
  else {
    return $class->$orig(@_);
  }
};

sub dump
{
  my $s = shift;
  my ($maxRow, $maxCol) = $s->lastUsedRowCol;

  print "mr $maxRow mc $maxCol\n";
  
  foreach my $r (1..$maxRow) {
    print join(',', map {
      $s->range($_, $r)->{Value} || ''
      } (1..$maxCol)) . "\n";
  }
}

#  give range by col, row or by "A1" notation
#
sub range
{
  my $s = shift;
  my ($r1, $r2) = @_;

  if (not defined $r2 and $r1 =~ /\w/) {
    return $s->ex->Range($r1);
  } else {
    return $s->ex->Range($s->colNumToName($r1).$r2);
  }
}

sub getType
{
  my $val = shift;

  if (defined $val and $val->isa("Win32::OLE")) {
    if ($val->{NumberFormat} ne 'General') {
      return "Date";
    }
    if (defined $val->{Value} and $val->{Value} =~ /^([+\-\.\s]|\d)+$/) {
      return "Number";
    }
    return "Text";
  } else {
    return "Text";
  }
}

=head2 sheet->setCellValue($range, $value)

=cut
sub setCellValue($$;$)
{
  my ($sheet, $range, $value) = @_;

#  $range->{NumberFormat} = 'General';

#  if (getType($range) eq 'Text') {
#    $range->{Text} = $value;
#  } elsif (getType($range) eq 'Date') {
#    $range->{Text} = $value;
#  } else {
    $range->{Value} = $value;
#  }
}

=head2 sheet->setCellValue(colNum, rowNum, $value)

=cut
#sub setCellValue($$$)
#{
#  my ($sheet, $colNum, $rowNum, $value) = @_;
#  croak "implement me";
#}

sub getCellValue
{
  my ($sheet, $colNum, $rowNum) = @_;
  my $val = '';

  my $range = colNumToName($colNum).$rowNum;
  my $cell = $sheet->Range($range);

  if (getType($cell) eq 'Text') {
    $val = $cell->{Text} || '';
    $val =~ s/^\s+//g;
    $val =~ s/\s+$//g;
    $val =~ s/  / /g;

  } elsif (getType($cell) eq 'Date') {
    #  
    #  Excel outs these as yyyymmdd - mm/dd/yy for value
    #
    $val = sprintf("%s",$cell->{Text} || '');

  } else {
    $val = $cell->{Value} || '';
    $val =~ s/ //g;
  }
  return $val;
}

# aka convToBase26
#   now why would Excel itself return numbers for the columns here..
#
sub colNumToName
{
  my $s = shift;
  my $num = shift;
  $num--;  # excel starts at 1 rather than 0

  my ($r, $i, @val) = (0, 0, ());

  do {
    $r = ($num % 26);
    $num = int(($num -$r) / 26);
    if ($i > 0) {
      $val[$i++] = chr(64 + $r);
    } else {
      $val[$i++] = chr(65 + $r);
    }
  } while ($num > 0);
  join('', reverse(@val));
}


sub lastUsedRowCol
{
  my $s = shift;

  my @rowcol = ();

  my $lr = $s->ex->UsedRange->Find({What=>"*",
      SearchDirection=>xlPrevious,
      SearchOrder=>xlByRows});

  if (not defined $lr) {
    @rowcol = (0,0);
    return @rowcol;
  } else {
    push @rowcol, $lr->{Row};
  }

  push @rowcol, $s->ex->UsedRange->Find({What=>"*", 
      SearchDirection=>xlPrevious,
      SearchOrder=>xlByColumns})->{Column};

  return @rowcol;
}

no Moose;
__PACKAGE__->meta->make_immutable;

#
#  IS-A
#
# instantiate with an excel ole object 
#   set a file
#
package Workbook;

use Try::Tiny;
use Moose;
use Moose::Util::TypeConstraints;
use Tie::IxHash;
use Win32::OLE qw(in with);
use Data::Dumper;
use Carp qw/confess croak cluck/;

extends 'Win32::OLE::Excel';

around 'BUILDARGS' => sub {
  my $orig = shift;
  my $class = shift;

  if ( @_ == 1 ) {
    if (ref $_[0] ) {
      return $class->$orig(ex => $_[0]);
    } else {
      return $class->$orig(file => fileName->new($_[0]));
    }
  } else {
    return $class->$orig(@_);
  }
};


#  instantiate with either another Workbook, and ex
#
#  Workbook->new({file => fileName->new("h:\\nate\\src\\mssql_json_excel\\data\\template.xlsx")});
#
#
sub BUILD {
  my $self = shift;
  my $book;

  if (@_ and ref($_[0]) eq 'HASH') {
      $self->file($_[0]->{file});
      try {
        $self->DEBUG("open ".$self->file->name);
        $book = $self->ex->Workbooks->Open($self->file->name);
        $self->ex($book);

      } catch {
        print "Couldn't open file ".$self->file->name."! $_\n";
        die;
        return undef;
      };
  } else {
    $book = $self->ex->Workbooks->Add;
    $self->ex($book);
  }
}

has '_sheets' => (
  traits => ['Hash'],
  is => 'rw',
  isa => 'HashRef[Sheet]', 
  default => sub {
    my $s = shift;
    my %s;
    tie %s, 'Tie::IxHash';
    foreach (1..$s->Worksheets->{Count}) {
      $s->DEBUG($s->Worksheets($_)->{Name});
      foreach (1..$s->Worksheets->{Count}) {
        $s{$s->Worksheets($_)->{Name}} = Sheet->new($s->Worksheets($_));
      }
    }
    \%s;
  },
  lazy => 1,
  handles => {
    sheet => 'get',
    addSheet => 'set',
    sheets => 'values',
    numSheets => 'count',
    sheetNames => 'keys',
    namesAndSheets => 'kv',
  },
);

has 'file' => (
    isa => 'fileName',
    is => 'rw',
    lazy => 1,
    default => sub { fileName->new('Book1.xls') },
    predicate => 'hasFile',
    writer => 'set_file',
    reader => 'get_file',
  );

#has 'sheets'

#  must handle Str, hash of fileName, and straight fileName
#  to cover construction and regular method use
#
sub file
{
  my $s = shift;

  return $s->get_file
    unless @_;

  my $f = $_[0];
  if (ref($f) and $f->isa('fileName')) {
  } else {
    $f = fileName->new($f);
  }
  $s->set_file($f);

#  my $book = $s->Workbooks->Open($s->_file->name);
#  bless($book, 'Workbook');

}

#
#  lookup Sheet by Excel's name
#  rather than filename->base
#
sub sheetByName
{
  my $s = shift;
  my $Pname = shift;

  foreach my $pair ( $s->namesAndSheets ) {
    my ($name, $sheet) = ($pair->[0], $pair->[1]);
    if ($name eq $Pname) {
      return $sheet;
    }
#    print $sheet->dump;
  }
  confess "Cannot find sheet $Pname!";
}

no Moose;
__PACKAGE__->meta->make_immutable;

#
#  openBook
#    
#
#
package ExcelOLE;

use strict;
use warnings;

use Data::Dumper;

use Win32::OLE qw(in with);
use Win32::OLE::Const 'Microsoft Excel';
use Win32::OLE::Variant;
use Win32::OLE::NLS qw(:LOCALE :DATE);

use Try::Tiny;
use Moose;
use Moose::Util::TypeConstraints;

#use Log::Log4perl qw/:easy/;

extends 'Win32::OLE::Excel';

#has 'logger' => (isa => 'Log::Log4perl', is => 'ro', lazy => 1,
#  default => sub { get_logger("ExcelOLE"); }
#);

sub open
{
    my $s = shift;
    my $file = shift;
    $s->Workbooks->Open($file);
}

has '_books' => (
  traits => ['Hash'],
  is => 'rw',
  isa => 'HashRef[Workbook]', 
  default => sub { {} },
  handles => {
    book => 'get',
    addBook => 'set',
    closeBook => 'delete',
    openFiles => 'keys',
    allBooks => 'values',
    numBooks => 'count',
    books => 'kv',
  },
);

sub openBook
{
  my ($s, $file) = @_;

  my $book = Workbook->new($file);
  $s->addBook($book->file->base => $book);
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
