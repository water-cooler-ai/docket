database_url = System.fetch_env!("DOCKET_BENCH_DATABASE_URL")
uri = URI.parse(database_url)
database = uri.path |> String.trim_leading("/")

credentials =
  case uri.userinfo && String.split(uri.userinfo, ":", parts: 2) do
    nil -> []
    [username] -> [username: URI.decode(username)]
    [username, password] -> [username: URI.decode(username), password: URI.decode(password)]
  end

connection =
  [
    hostname: uri.host,
    port: uri.port || 5432,
    database: database,
    sync_connect: true,
    ssl: [verify: :verify_none]
  ] ++ credentials

channel = "docket_bench_probe"
payload = Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)

{:ok, listener} = Postgrex.Notifications.start_link(connection)
{:ok, reference} = Postgrex.Notifications.listen(listener, channel)
{:ok, sender} = Postgrex.start_link(connection)
_result = Postgrex.query!(sender, "SELECT pg_notify($1, $2)", [channel, payload])

receive do
  {:notification, ^listener, ^reference, ^channel, ^payload} ->
    IO.puts("received=1 payload_matched=true")
after
  5_000 ->
    IO.puts("received=0 payload_matched=false")
    System.halt(1)
end
