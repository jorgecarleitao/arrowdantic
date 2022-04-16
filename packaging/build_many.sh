set -x
yum install -y unixODBC
maturin build --strip --release --manylinux 2010
