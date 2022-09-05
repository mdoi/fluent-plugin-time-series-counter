# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-time-series-counter"
  spec.version       = '0.1.1'
  spec.authors       = ["Masayuki DOI"]
  spec.email         = ["dotquasar@gmail.com"]
  spec.description   = "plugin for counting multiple records and output time-series value"
  spec.summary       = spec.description
  spec.homepage      = "https://github.com/mdoi/fluent-plugin-time-series-counter"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'fluentd', '~> 0.14.0'

  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'test-unit'
end
