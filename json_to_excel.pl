=head1 NAME

  json to excel (and back again)

=head1 SYNOPSIS

  this is the server side code utilizing the re-written 
  ExcelOLE.pm that speaks a basic transmission protocol
  This simple protocol encapsulates a list of worksheets and cell data

=head1 DESCRIPTION

  Listens on a socket, spins off a connected scoket to processReabale
  Process Readable creates a new spreadsheet based on protocol updates of an on disk template

=cut

use strict;
#use Test::More;
use IO::Socket;
use IO::Select;
use JSON;
use Storable qw/nstore retrieve/;
use Tie::IxHash;
use Try::Tiny;
use ExcelOLE;
use Digest::MD5 qw/md5_hex/;
use Carp qw/croak carp confess/;
use MIME::Base64;
use POSIX qw/strftime/;
use IO::Handle;
use open IN => ':bytes', OUT => ':bytes';
use lib '.';

use Data::Dumper;

my $GdataDir = "c:\\mssql_json_excel\\data";
my $GD = "\\";
my $Gloglvl = 3;

sub DESTROY
{
  unlink basename($0).".pid";
}

#  or just dup stdout
open(LOG, ">", "$GdataDir$GD"."server.log") || die "wtf";

my $Gjson = JSON->new;
my $CHUNKSIZE = 1024*1024;

sub ok ($;$) 
{
  my ($cond,$mess) = @_;
  if( $cond ){
    INFO(sprintf("OK\t$mess"));
  } else{
    INFO(sprintf("NOT OK\t$mess"));
  }
}

sub _OUT {
  my ($msg, @fmt) = @_;
#  printf ( strftime("%Y-%m-%d %H:%M:%S\t", localtime()). "$msg\n", @fmt);

  unless (fileno(LOG)) {
    open(LOG, ">>", "$GdataDir$GD"."server.log") || die "wtf";
  }
  printf ( LOG strftime("%Y-%m-%d %H:%M:%S\t", localtime()). "$msg\n", @fmt);
}

sub DEBUG
{
  my ($msg, @fmt) = @_;
  if($Gloglvl >= 3) {
    _OUT($msg, @fmt);
  }
}

sub CRUFT
{
  my ($msg, @fmt) = @_;
  if ($Gloglvl >= 4) {
    _OUT($msg, @fmt);
  }
}

sub INFO
{
  my ($msg, @fmt) = @_;
  if ($Gloglvl >= 2) {
    _OUT($msg, @fmt);
  }
}

sub ERROR
{
  my ($msg, @fmt) = @_;
  if ($Gloglvl >= 1) {
    _OUT($msg, @fmt);
  }
}

=head2 nestedArrayToHash($calar)
  perform some data validation on George/Nate excel protocol
  re-organize into a simple hash in doing so

=cut
sub _jsonToHash($)
{
  my $decoded = shift;
  my %out;
  my $token;

  my @keys = %$decoded;

  foreach my $key (@keys) {
    my $cells = $decoded->{$key};
    while (keys @$cells) {
      my $token = shift @$decoded;
      my $data = shift @$decoded;

      #  cells array
      #
      if (ref($data)) {
        foreach my $tuple (@$data) {
          # cell=>{cellAddress} = value;
          $out{$token}->{$tuple->[0]} = $tuple->[1];
        }
      } elsif (exists $out{$token}) {
        confess "$token already exists! Yell at George for garbage data";
      } else {
        $out{$token} = $data;
      }
    }
  }
  \%out;
}

#  ghetto singleton
#  aka, private block execute once upon runtime
#
{
  my $ex = ExcelOLE->new();

#  $ex->ex;

=head1 METHODS

=head2 createSpreadsheet($wirePacket)
  takes wire packet ( a json decoded scalar )
  compares sheetnames as the first data set in George/Nate protocol
    dies if there is a mismatch in expected / actual
  updates cell values per sheet from the second data set in George/Nate

=cut

  sub createSpreadsheet($$)
  {
    my ($PwirePacket, $PoutFileName) = @_;

    my $derefHash = ${$PwirePacket};
#    print Dumper($derefHash);

    my $wireSheets = $derefHash->{worksheetnames};
    my $wireWorkbook = $derefHash->{worksheets};
    
    $ex->openBook($GdataDir.$GD."template.xlsx");
    my $book = $ex->book('template.xlsx');

#    foreach my $buf ($ex->openFiles()) {
#      print "open file $buf\n";
#    }

    #  compare sheet names
    #
#    foreach my $sheetName (keys %$wireSheets) {
#      if ($Gdebug) {
#        print "called sheet by name ".$sheetName."\n";
#      }
#      my $book = $ex->book('template.xlsx');
#      my $sheet= $book->sheetByName($sheetName,1);
#    }

#    print Dumper(\$wireWorkbook);
    foreach my $wireWorksheetName (keys %$wireWorkbook) {
      my $wireCells = $wireWorkbook->{$wireWorksheetName};
      my $sheet= $book->sheetByName($wireWorksheetName,1);
      confess "template.xlsx doesn't contain $wireWorksheetName, contact Sonal" unless (defined $sheet);

      foreach my $cellAddress (keys %$wireCells) {
        my $cellValue = $wireCells->{$cellAddress};
        CRUFT("$wireWorksheetName:$cellAddress = $cellValue");

        my $cell = $sheet->range($cellAddress);
        $sheet->setCellValue($cell, $cellValue);
      }
    }
  
    #$book->SaveAs("blah.xls");
    $book->SaveAs($GdataDir.$GD.$PoutFileName);

    $book->Close();

    my $ctx = Digest::MD5->new;

    open( BOOK, "<:bytes", $GdataDir.$GD.$PoutFileName ) || confess "Cannot read $GdataDir.$GD.$PoutFileName";

    my @nfo = stat(BOOK);
    my $byteSize = $nfo[7];

    my $byteStream;
    my $bc = sysread(BOOK, $byteStream, $byteSize);
    ok($bc == $byteSize, "file size sysread matches stat (which is $bc)");

    close BOOK;

    unlink $GdataDir.$GD.$PoutFileName;

    DEBUG("bytestream is length[".length($byteStream)."] bc $bc\n");

    DEBUG("money shot hex: ".md5_hex($byteStream));

    return $byteStream;
  }
}

=head2 readExcelUpdates($socketFileHandle)
=head2 sendSpreadsheet($socketFileHandle, $dataToWrite)

=cut

sub readExcelUpdates($)
{
  my $fh = shift;
  my $perl_scalar = undef;

  my @obj;
  my $buf;
  my $done = 0;
  my $retry = 15;

  my $sel = new IO::Select( $fh );
  #
  # retry aka #of chunks + retry
  #
  while ($sel->can_read(2) and not $done and $retry-- > 0) {
    my $bR = sysread($fh, $buf, $CHUNKSIZE);
    ok(length($buf) >0, "read $bR bytes from client, sock $fh");
    if ($bR == 0) {
      ok(1, "eof from client");
      $done = 1;
    } else {
      push @obj, $buf;
    }
  }
  my $jsonEncoded = join("", @obj);

  if (defined $jsonEncoded) {
    #$perl_scalar = jsonArrayRefToHash($json->decode($jsonEncoded));
    eval {
      $perl_scalar = $Gjson->decode($jsonEncoded);
    };

    if (not sendMSG($sel, "ACK")) {
      ok(1, "7669: implement a state change here");
    }
  }
  \$perl_scalar;
}

sub hold($)
{
  my $seconds = shift;
  my $epoch = time;
  do { } until (time > ($epoch + $seconds)); 
}

sub sendMSG($$)
{
  my ($sel,$msg) = @_;
  my ($retry, $done, @fh) = (5, 0);

  while (my @fh = $sel->can_write(1) and not $done and $retry-- > 0) {
    foreach my $fh (@fh) {
      my $bW = syswrite($fh, $msg, 3);
      ok($bW > 0, "sent $msg");
      if ($bW > 0) { $done = 1 } else { hold(1); };
    }
  }
  $done;
}

sub readRSP($)
{
  my $sel = shift;
  my ($count, $res, $done, $buf, @fh) = (5, 'NOP', 0, undef);

  while (my @fh = $sel->can_read(1) and not $done and $count-- >= 0) {
    foreach my $fh (@fh) {
      ok($fh, 'waiting for client response (on $fh)');
      my $bR = sysread($fh, $buf, 3);
      if ($bR == 3 and ($buf eq 'ACK' or $buf = 'FOO')) {
        $res = $buf;
        $done = 1;
      } else {
        $count--;
        ok(1, "received garbage [$buf], waiting $count");
        hold(1);
      }
    }
  }
  $res;
}

sub sendData($$;$)
{
  my ($sel, $data, $Psize) = @_;
  my ($retry, $done, $size, @fh) = (5, 0);

  $size = $Psize || length($data);

  while (@fh = $sel->can_write(0.5) and not $done) {
    foreach my $fh (@fh) {
      my $b = syswrite($fh, $data, $size);
      return $b;
    }
  }
}

sub readData($$;$)
{
  my ($sel, $dataRef, $Psize, $PtotSize) = @_;
  my ($retry, $done, $data, $bR, $size, @fh) = (5, 0);

  $size = $Psize; # || CHUNKSIZE

  while (@fh = $sel->can_read(0.5) and not $done) {
    foreach my $fh (@fh) {
      $bR = sysread($fh, $data, $size);
      ${ $dataRef } = $data;
    }
  }
}

sub sendSpreadsheet($$)
{
  my ($fh, $data) = @_;

  my $buf;
  my $count = 20;
  my $done = 0;
  my $sel = new IO::Select( $fh );

  my $state = "WRITE";
  while (($state eq "WRITE" or $state eq "READ") and ($count-- > 0)) {
    if ($state eq "WRITE") {
      my $r = sendData($sel, sprintf("%16.0d",length($data)), 16);
      if ($r) {
        ok($r, "wrote size [".sprintf("%16.0d",length($data)). "]to client, ($r bytes)");
        $r = sendData($sel, $data, length($data));
        if ($r > 0) {
          ok($r, "wrote $r to client");
        }

        if (not sendMSG($sel, 'ACK')) {
          ok(1, "7569: implement a state change here");
        }
        $state = "READ";
      }
    }
    if ($state eq "READ") {
      my $r = readRSP($sel);
      if ($r eq "ACK") {
        ok($b, "received checksums match ACK");
        $state = "DONE";
      } elsif ($r eq "FOO") {
        ok($r, "received FOO, re-send");
        $state = "WRITE";
        $count = 20;
      } elsif ($r eq "NOP") {
        $state = "FAIL";
      }
    }
    
    ok($state, "state $state");
  }
  $state;
}

##################################################################
#  MAIN LOOP
#
my $lsn = new IO::Socket::INET(Listen => 1, LocalPort => 31337);

ok (defined $lsn, "server socket is listening");
if (not defined $lsn) {
  exit;
}

my $sel = new IO::Select( $lsn );

=head1
  basic server operation:
  read from initial socket connection from client
  create a bytestream containing data
  write to second socket connection from client
  rinse/wash/repeat

=cut

my $jsonEncoded = undef;
my ($sockCli, $readFromWire, $jsonEncoded) = ((undef) x 4);
while (1) {
  my @readFH = $sel->can_read;
  foreach my $fh (@readFH) {
    if($fh == $lsn) { 
      # Create a new socket
      my $new = $lsn->accept;

      $sockCli = $new;
      $sel->add($sockCli);

      ok($sockCli, "accepted connection from ".$sockCli->peerhost); 
    } elsif ($fh == $sockCli) {
      ok (defined $fh, "read excel updates json obj $fh");

      # Process socket
      $readFromWire = readExcelUpdates($fh);
      ok(defined $readFromWire,'decoded json object ' . length($readFromWire)." bytes");

#      close($fh);
#      $sel->remove($reader);
#      $fh->close;
#      $reader = undef;
#      $fh = undef;

    }
  }
  #
  my @writeFH = $sel->can_write;
  foreach my $fh (@writeFH) {
    if ($fh == $sockCli and defined $readFromWire) {
      ok (defined $fh, "creating spreadsheet $fh");
      #  memory address of socket as filename
      #
      $fh =~ /.*?0x(.*?)\)/;
      my $fileName = "_$1.xlsx";
#      my $fileName = strftime("%s", localtime())."_$1.xlsx";
#      ok($fileName, "local cache $fileName");
      ##

      my $rawSheet = createSpreadsheet($readFromWire, $fileName);

      DEBUG("sizeof rawSheet ". length($rawSheet));
      my %doc = (
        bytes => encode_base64($rawSheet),
        checksum => md5_hex($rawSheet),
      );

      $jsonEncoded = $Gjson->encode(\%doc);
      ok (defined $fh, "writing to client checksum[".$doc{checksum}."], sock $fh, ".length($jsonEncoded));
      #
      my $r = sendSpreadsheet($fh, $jsonEncoded);

      ok($r eq "DONE", "successfully sent spreadsheet!");

      $sel->remove($sockCli);
      $sockCli->close;
      $sockCli = undef;

      $jsonEncoded = undef;
      $readFromWire = undef;
    }
  }
}

done_testing();

=head1 AUTHOR

  Nathaniel Lally 2013

=cut
