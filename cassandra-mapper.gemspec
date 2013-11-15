Gem::Specification.new do |gem|
  gem.name          = 'cassandra-mapper'
  gem.version       = '0.3'
  gem.authors       = 'brainopia'
  gem.email         = 'brainopia@evilmartians.com'
  gem.homepage      = 'https://github.com/brainopia/cassandra-mapper'
  gem.summary       = 'Cassandra mapper'
  gem.description   = <<-DESCRIPTION
    Work with cassandra in datamapper style.
  DESCRIPTION

  gem.files         = `git ls-files`.split($/)
  gem.test_files    = gem.files.grep %r{^spec/}
  gem.require_paths = %w(lib)

  gem.add_dependency 'cassandra', '~> 0.18.0'
  gem.add_dependency 'murmurhash3-ruby'
  gem.add_development_dependency 'rspec'
end
