class PQRS < Formula
  desc "Apache Parquet command-line tools and utilities"
  homepage "https://github.com/manojkarthick/pqrs"
  url "https://github.com/manojkarthick/pqrs/releases/latest/download/pqrs-mac.tar.gz"
  sha256 "c5c0ad8f3763c85173801f9eff2e209c34f22798e3606f65928f99a6c5c00d0f"
  version "0.1.1"

  def install
    bin.install "pqrs"
  end

end