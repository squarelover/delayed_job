require 'mongoid'
require File.join(File.dirname(__FILE__), '../../../../../config/initializers/database')

::DialogCentral::Database.uri = "mongodb://localhost:27017/delayed_job"

unless defined?(Story)
  class Story
    include ::Mongoid::Document
    def tell; text; end
    def whatever(n, _); tell*n; end
    def self.count; end
  
    handle_asynchronously :whatever
  end
end
