require 'timeout'
require 'mongo'

module Rack
  
  class GridFSConnectionError < StandardError ; end
  
  class GridFS

    attr_reader :hostname, :port, :database, :prefix, :db, :user, :password
    
    def initialize(app, options = {})
      options = {
        :hostname => 'localhost',
        :prefix   => 'gridfs',
        :port     => Mongo::Connection::DEFAULT_PORT
      }.merge(options)

      @app        = app
      @hostname   = options[:hostname]
      @port       = options[:port]
      @database   = options[:database]
      @prefix     = options[:prefix]
      @db         = nil
      @user       = options[:user]
      @password   = options[:password]

      connect!
    end

    def call(env)
      request = Rack::Request.new(env)
      if request.path_info =~ /^\/#{prefix}\/(.+)$/
        gridfs_request($1, request)
      else
        @app.call(env)
      end
    end

    def gridfs_request(id, request)
      grid = Mongo::GridFileSystem.new(db)
      file = grid.open(id, 'r')
      if request.env['If-None-Match'] == file.files_id.to_s || request.env['If-Modified-Since'] == file.upload_date.httpdate
        [304, {'Content-Type' => 'text/plain'}, ['Not modified']]
      else
        [200, {'Content-Type' => file.content_type, 'Last-Modified' => file.upload_date.httpdate, 'Etag' => file.files_id.to_s}, [file.read]]
      end
    rescue Mongo::GridError, BSON::InvalidObjectId
      [404, {'Content-Type' => 'text/plain'}, ['File not found.' + id]]
    rescue Mongo::GridFileNotFound
      [404, {'Content-Type' => 'text/plain'}, ['File not found.']]
    end
    
    private
    
    def connect!
      Timeout::timeout(5) do
        @db = Mongo::Connection.new(hostname, @port).db(database)
        if @user and @password
          @db.authenticate(@user, @password)
        end
        @db
      end
    rescue Exception => e
      raise Rack::GridFSConnectionError, "Unable to connect to the MongoDB server (#{e.to_s})"
    end
    
  end
    
end
