## KeystoneML release process

1. We publish KeystoneML releases and snapshots to sonatype repository https://oss.sonatype.org/. To
create a release you will first need to create a SonaType account.

2. Create and distribute a gpg key if you don't have one. The steps to do that are:
```
  gpg --gen-key # Follow the prompts to create a key
  # Publish the key to a keyserver. If this doesn't work go to https://sks-keyservers.net/i/
  # and paste the output of gpg --armor --export <email-id-used-in-previous-step>
  gpg --send-keys 
``` 

3. Add `sbt-pgp` and `sbt-sonatype` to your plugins at `~/.sbt/0.13/plugins/gpg.sbt`
```
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")
```

4. Add your Sonatype credentials to `~/.sbt/0.13/sonatype.sbt` 
credentials += Credentials("Sonatype Nexus Repository Manager",
                           "oss.sonatype.org",
                           "<userame>",
                           "<password>")

5. Run `sbt/sbt publishSigned`. This should be sufficient for a snapshot release. To use a snapshot
release users will need to add the Sonatype snapshot repository to their list of resolvers. In SBT
it looks like
```scala
resolvers ++= Seq(
  "Sonatype staging" at "https://oss.sonatype.org/content/repositories/snapshots",
  ... // other resolvers
  )
```

## TODO(tomerk): Add details about publishing a full release
