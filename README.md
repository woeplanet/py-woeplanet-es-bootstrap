# <img src="https://avatars1.githubusercontent.com/u/29209318?s=32&v=4" width="32" height="32" alt="woeplanet">&nbsp;py-woeplanet-es-bootstrap

# The Really Short Version

_Forking GeoPlanet one place type at a time_.

# The Short Version

WoePlanet is Where On Earth (AKA WOE, also AKA GeoPlanet) data, smushed up with coordinate and boundary data from Flickr Shapes, Quattroshapes and Natural Earth Data (that's fancy talk for _polygons_) as well as concordances and other metadata rescued from `woe.spum.org` before it died and went offline.

# The Longer Version

This is probably not the repo you want.

Simple Python wrapper for accessing WoePlanet/GeoPlanet placetypes in Elasticsearch.
```
sudo cp ./etc/*.plist /Library/LaunchDaemons/
sudo chown root:wheel /Library/LaunchDaemons/limit.*.plist
sudo chmod 644 /Library/LaunchDaemons/limit.*.plist
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
sudo launchctl load -w /Library/LaunchDaemons/limit.maxproc.plist
```

Edit `/usr/local/etc/elasticsearch/jvm.options`:

```
-Xms2g
-Xmx2g
```
