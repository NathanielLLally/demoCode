package RTP;
=head1 NAME

  Rudementary Transfer Protocol

=head1 DESCRIPTION

  basic socket transmission implementation where the usa case was an in memory object being sent to a web server without a persistent environment where updating CPAN modules will cascade into much testing overhead

=head1 USAGE

sub newSock {
  print "opening socket to $ip\n";
  my $port = 31337;
  my $cli = IO::Socket::INET->new(
    PeerAddr => $ip || '127.0.0.1',
    PeerPort => $port, 
    Proto => 'tcp'
      );
  $cli;
}

my $rtp = RTP->new(sock => newSock());

sub Send($$)
{
  my ($Prtp, $Pdata)  = @_;
  my ($state);

  try {
    $state = $Prtp->sendData($Pdata);
  } catch {
    warn "sendData failed: $_, trying again";
    $Prtp->close();
    $Prtp->sock(newSock());
    Send($Prtp, $Pdata);
  };
}

foreach my $obj (@DATA) {

  my ($data, $size, $sent);
  $data = Storable::freeze($obj);
  Send($rtp, $data);
}


=cut

use POSIX qw/strftime/;
use IO::Handle;
use IO::Select;
use Moose;
#use Log::Log4perl;
use Try::Tiny;
use Digest::MD5 qw/md5_hex/;
use open IN => ':bytes', OUT => ':bytes';
use Test::More;
use Time::HiRes qw/usleep/;
use Data::Dumper;

#has 'sock' => (isa => 'IO::Socket', is => 'rw', lazy => 1, 
# default => sub  {IO::Handle->new_from_fd(*STDIN, "<") }
has 'sock' => (isa => 'Any', is => 'rw' );

has 'sel' => (isa => 'IO::Select', is => 'rw', lazy => 1, 
  default => sub {
    my $s = shift;
    new IO::Select( $s->sock );
  }
);

#our $CHUNKSIZE = 10240;
#our $CHUNKSIZE = 18432;
our $CHUNKSIZE = 20480;
our $GretryCount = 5;

sub DESTROY()
{
  my $s = shift;
  $s->close();
}

sub BUILD
{
  my $s = shift;
  if (defined $s->sock) {
    $s->sel->add($s->sock);
  } 
}

sub DEBUG
{
  my ($s, $msg);
  printf ("%s %s:\n", POSIX::strftime("%D %H:%M:%S", localtime(time)), $msg) if (defined $msg);
}

=head2
  close()
    convenience function
=cut
sub close()
{
  my $s = shift;
  print "closing socket\n";
  if (defined $s->sock) {
    $s->sel->remove($s->sock);
    $s->sock->close();
  }
}

=head2
  hold($seconds)

  signalling is out for a wait, so thats any alarm based mechanism
  yield?
  Timer::HiRes
=cut
sub hold($)
{
  my $seconds = shift;
#  my $epoch = time;
#  do {
#  } until (time > ($epoch + $seconds)); 
  usleep(1000000 * $seconds);
}

=head2
  sendMSG($)
=cut
sub sendMSG($)
{
  my ($s,$msg) = @_;
  my ($retry, $done, @fh) = (5, 0);

  $s->DEBUG("entering sendMSG");
  while (my @fh = $s->sel->can_write(0.5) and not $done and $retry-- > 0) {
    foreach my $fh (@fh) {
      my $bW = syswrite($fh, $msg, 3);
      if (not defined $bW) {
        die "sendMSG";
      }
      ok($bW > 0, "sent $msg");
      if ($bW > 0) { $done = 1 } else { hold(0.25); };
    }
  }
  if (not $done) {
    die "sendMSG";
  }
  $s->DEBUG("exiting sendMSG");
  $done;
}

=head2
  $message = readRSP()
  recieve a 3 byte message
  at this time, we have
    ACK
    FOO
    NOP
    CNT
    FIN
=cut
sub readRSP()
{
  my $s = shift;
  my ($count, $res, $done, $buf, @fh) = (5, 'NOP', 0, undef);
  
  $s->DEBUG("entering receiveMSG");
  while ($res eq 'NOP') {
  while (my @fh = $s->sel->can_read(0.5) and not $done and $count-- >= 0) {
    foreach my $fh (@fh) {
      my $bR = sysread($fh, $buf, 3);
      if ($bR == 3 and ($buf eq 'ACK' or $buf eq 'FOO' or $buf eq 'FIN' or $buf eq 'NOP' or $buf eq 'CNT')) {
        $res = $buf;
        ok(1, "recieved $res");
        $done = 1;
      } else {
        $count--;
        ok(1, "received garbage [$buf], waiting $count");
        hold(0.25);
      }
    }
  }

  if (not $done) {
    die "readRSP";
  }
  $s->DEBUG("exiting receiveMSG");
}
  $res;
}

#  readsData from $self->sel into buffer up until size
#
sub _readData($)
{
  my ($s, $Psize) = @_;
  my ($retry, $done, $count, $data, $bR, $size, $out, $totsize, @fh) = (5, 0, 5);

  $size = $Psize; # || CHUNKSIZE

  while (@fh = $s->sel->can_read(2) and not $done and $count-- >= 0) {
    foreach my $fh (@fh) {
      $bR = sysread($fh, $data, $size);
      if (not defined $bR) { 
        $done = 1;
        print "got undef from sysread, FIN or RST?\n";
      } else {
        $totsize += $bR;
        $out .= $data;
      }
    }
    $done = 1 if ($totsize == $Psize);
  }
  if (not $done) {
    die "_readData";
  }
  $out;
}

sub _sendData($$;$)
{
  my ($s, $data, $Psize) = @_;
  my ($retry, $done, $size, @fh) = (5, 0);

  $size = $Psize || length($data);

  while (@fh = $s->sel->can_write(0.5) and not $done) {
    foreach my $fh (@fh) {
      my $b = syswrite($fh, $data, $size);
      if (not defined $b or $b <= 0) {
        die "yey\n";
      }
      return $b;
    }
  }
}

=head2
  sendData
  
  computes size, checksum.  writes size, checksum, data
  sendMsg Ack, waits for response.  retries or done

=cut
sub sendData($)
{
  my ($self, $Pdata) = @_;

  print "sending ".length($Pdata)." bytes in ";

  my @data;
  while (length($Pdata)) {
    push @data, substr($Pdata, 0 , $CHUNKSIZE, '');
  }

  printf( "%u chunks\n", $#data + 1);

#  print Dumper(Storable::thaw(join('', @data)));

  my ($data, $state, $count);
  while ($data = shift @data) {
    $state = "WRITE";
    $count = $GretryCount;
    printf( "%u chunks remain\n", $#data + 2);
    while (($state eq "WRITE") and ($count-- > 0)) {
        my $r;
        try {
          $r = $self->_sendData(sprintf("%16.0d",length($data)), 16);
        } catch {
          die "sending size: $_";
        };
        if ($r == 16) {
          ok($r, "wrote size [".sprintf("%16.0d",length($data)). "]to client, ($r bytes)");
        }

        my $chk = md5_hex($data);
        try {
          $r = $self->_sendData(sprintf("% 32s",$chk), 32);
        } catch {
          die "sending checksum: $_";
        };
        if ($r == 32) {
          ok($r, "wrote md5_hex [".sprintf("% 32s",$chk). "]to client");
        }
        try {
          $r = $self->_sendData($data, length($data));
        } catch {
          die "sending chunk: $_";
        };
        if ($r > 0 and $r == length($data)) {
          ok($r, "wrote $r to client");
        }
        if ($#data >= 0) {
          try {
            if (not $self->sendMSG('CNT')) {
              ok(1, "7569: implement a state change here");
            }
          } catch {
            die "sending CNT $_";
          };
        } else {
          try {
            if (not $self->sendMSG('FIN')) {
              ok(1, "7569: implement a state change here");
            }
          } catch {
            die "sending FIN";
          };
        }

        try {
          $r = $self->readRSP();
        } catch {
          die "reading RSP: $_";
        };
        if ($r eq "ACK") {
          ok($r, "received checksums match ACK");
          if ($#data  >= 0)  {
            $state = "NEXT";
          } else {
            $state = "DONE";
          }
        } elsif ($r eq "FOO") {
          ok($r, "received FOO, re-send");
          $state = "WRITE";
          $count = 20;
        } elsif ($r eq "NOP") {
          die "NOP recieved??";
          $state = "FAIL";
        }

      ok($state, "state $state");
    }
  }
  $state;
}

=head2 readData(size)

=cut
sub readData()
{
  my $s = shift;
  my ($wireSize, $wireChecksum, $wireChunk, $wireObj) = ((undef) x 4);
  my @chunks;
  my $state = "READ";
  while ($state eq "WRITE" or $state eq "READ") {
    if ($state eq "READ") {
      ok(defined $s->sock, 'waiting for server');

      my $r;
      do {
        ($wireChunk, $wireSize, $wireChecksum ) = ((undef) x 3);
          print "waiting on size\n";
        try {
          $wireSize = $s->_readData(16);
        } catch {
          die "failed reading size: $_";
        };

        try {
          $wireChecksum = $s->_readData(32);
        } catch {
          die "failed reading checksum: $_";
        };
        ok($wireChecksum, "got md5 [$wireChecksum] from server");

        try {
          $wireChunk = $s->_readData($wireSize);
        } catch {
          die "failed reading chunk: $_";
        };

        if ($wireSize == length($wireChunk)) {
          ok(1, "size $wireSize matches bytes received");
          $state = "WRITE";
        }

        try {
          $r = $s->readRSP();
        } catch {
          die "failed readRSP :$_";
        };
        if ($r eq 'CNT' or $r eq 'FIN') {
          my $computedChecksum = md5_hex($wireChunk);
          ok($computedChecksum eq $wireChecksum, "checksums match, sending ACK");
          if ($computedChecksum eq $wireChecksum and length($wireChecksum) > 0 ) {
            push @chunks, $wireChunk;

            try {
              if ($s->sendMSG("ACK")) {
                $state = "DONE" if ($r eq 'FIN');
              }
            } catch {
              die "failed sendMSG: $_";
            };

          } else {
            ok(1, "checksum mismatch [$computedChecksum] != [$wireChecksum], sending FOO for retry");

            try {
              if ($s->sendMSG("FOO")) {
                $state = "READ";
              }
            } catch {
              die "failed sendMSG: $_";
            };
          }
        }
      } while ($r eq 'CNT' or $r eq 'FOO');

    }
  }
  join('', @chunks);
}

$SIG{PIPE} = sub {
  die "sigpipe!";
};

1;
