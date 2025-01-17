lein do clean, run test \
  --tarball http://aphyr.com/media/cockroach-2017-03-30.tar.bz2 \
  --username admin \
  --nodes-file ~/nodes \
  --test sets \
  --nemesis majority-ring \
  --nemesis2 subcritical-skews \
  --time-limit 180 \
  --test-count 10
