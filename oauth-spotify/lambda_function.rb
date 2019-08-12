require 'json'
require 'date'
require 'aws-sdk'
require 'rspotify'
if (!ENV['AWS_SESSION_TOKEN'])
  require 'byebug'
end

class PlaylistUpdater

  attr_accessor :secretsmanager, :spotify_user, :credentials, :secret_id

  def initialize(region, secret_id)
    @secretsmanager=(Aws::SecretsManager::Client.new(region: region))
    @secret_id = secret_id
    @credentials = get_secret(region)
  end

  def get_secret(region)
    get_secret_value_response = self.secretsmanager.get_secret_value(secret_id: self.secret_id)
    return JSON.parse(get_secret_value_response.secret_string)
  end

  def put_secret(new_secrets)
    self.secretsmanager.put_secret_value(
      {
        secret_id: self.secret_id,
        secret_string: new_secrets.to_json.to_s
      })
  end

  def save_new_access_token(new_access_token, time_to_die)
    self.credentials["access_token"] = new_access_token
    self.credentials["token_expiration"] = time_to_die
    self.put_secret(self.credentials)
  end

  def create_spotify_user   #(spotify_access)

    result = RSpotify.authenticate(
      self.credentials["client_id"],
      self.credentials["client_secret"])

    # The RSpotify code is smart enough to handle the "access token expired"
    # exception and then request a new token via the refresh token.
    # In that event, we need to save the token in the SecretsManager.

    callback_proc = Proc.new { |new_access_token, token_lifetime |
       now = Time.now.utc.to_i  # seconds since 1/1/1970, midnight UTC
       time_to_die = now+token_lifetime
       #puts("new access token will expire at #{Time.at(deadline).utc.to_s}")
       self.save_new_access_token(new_access_token, time_to_die)
     }

    self.spotify_user = RSpotify::User.new(
      {
        'credentials' => {
           "token" => self.credentials["access_token"],
           "refresh_token" => self.credentials["refresh_token"],
           "access_refresh_callback" => callback_proc
        } ,
        'id' => self.credentials["user_id"]
      })

    return self.spotify_user

  end

  def retrievePlaylist(playlist_id)

    begin
      results = self.spotify_user.playlists()
    rescue RestClient::BadRequest, RestClient::Unauthorized => e
      #puts("#{e.response.code} -> response = '#{e.response}', #{e.to_s}")
      return {
        statusCode: e.response.code,
        body: { "message": e.message }.to_json
      }
    end

    playlist = results.detect { |pl| pl.id == playlist_id }
    return playlist

  end

  def uploadNewTracksToPlaylist(playlist, track_ids)
    # max number of tracks per API call is 100
    # https://developer.spotify.com/documentation/web-api/reference/playlists/replace-playlists-tracks/
    #
    # This function will handle the case when the number of tracks exceeds the max value for one
    # invocation of the API. In that case, the function will upload the first 100, then invoke
    # #add_tracks until all the tracks are uploaded to the playlist.
    #
    track_limit = 50

    new_tracks = track_ids.collect { |id|
      RSpotify::Track.new({ 'uri' => "spotify:track:#{id}"})}

    total = new_tracks.length
    start_index = 0
    stop_index = ([start_index+track_limit, total].min) - 1

    begin
      execute_with_retry { playlist.replace_tracks!(new_tracks[start_index..stop_index]) }
    rescue RestClient::InternalServerError => e
      # weirdly bad code in RestClient raises exception after printing this error
      #    warning: Overriding "Content-Type" header "application/json"
      #    with "application/x-www-form-urlencoded" due to payload

      # ignore this for now
    end

    processed = stop_index - start_index
    start_index = stop_index+1

    # https://www.rubydoc.info/github/guilhermesad/rspotify/master/RSpotify/Playlist#add_tracks!-instance_method

    while (start_index < total)

      stop_index = ([start_index+track_limit, total].min) - 1
      execute_with_retry { playlist.add_tracks!(new_tracks[start_index..stop_index]) }
      processed = processed+(stop_index - start_index)+1
      start_index = stop_index+1

    end

    return playlist.snapshot_id  # for lack of anything else

  end

  def execute_with_retry(&block)
    # Attempt to execute the block. Spotify will raise the TooManyRequests
    # when that situation occurs. The exception includes the minimum seconds
    # that we need to wait before trying again.
    # The function sets a limit on retries just in case the exception is
    # getting raised indefinitely, i.e. something is haywire in Spotify.

    retries = 0
    limit = 20  # random choice

    while true
      begin
        results = block.call  #yield
        return results
      rescue RestClient::TooManyRequests => e
        if (retries > limit)
          raise  # too many retries. giving up.
        end
        zzz = (e.http_headers[:retry_after]).to_i
        sleep(zzz+1)  # wait before trying again
        retries = retries+1
      end
    end
  end
end


def lambda_handler(event:, context:)

  region = ENV['AWS_DEFAULT_REGION']
  secrets_name = ENV['SPOTIFY_ACCESS']
  # should raise exception if either of the above values are missing

  updater = PlaylistUpdater.new(region, secrets_name)

  spotify_user = updater.create_spotify_user()
  playlist_id = event["spotify_playlist_id"]
  track_ids = event["spotify_track_ids"]  # rewrote to expect a JSON array

  playlist = updater.retrievePlaylist(playlist_id)

  if (playlist.nil?)
    return {
      statusCode: 400,
      body: { "message": "could not find playlist with id #{playlist_id}" }.to_json }
  end

  snapshot_id = updater.uploadNewTracksToPlaylist(playlist, track_ids)

  return {
    statusCode: 200,
    body: {
      "snapshot_id": snapshot_id,
      "message": "saved #{track_ids.length} tracks to playlist #{playlist_id}"
    }.to_json
  }

end
