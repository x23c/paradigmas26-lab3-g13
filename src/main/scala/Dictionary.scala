object Dictionary {

  def loadFromFile(filePath: String, entityType: String): Option[List[NamedEntity]] = {
    FileIO.readDictionaryFile(filePath).map { lines =>
      lines.map { name =>
        entityType match {
          case "Person"              => new Person(name)
          case "Organization"        => new Organization(name)
          case "University"          => new University(name)
          case "Place"               => new Place(name)
          case "Technology"          => new Technology(name)
          case "ProgrammingLanguage" => new ProgrammingLanguage(name)
          case _                     => new Person(name)
        }
      }
    }
  }


  def loadAll(entitiesDir: String): List[NamedEntity] = {

    val dir = new java.io.File(entitiesDir)

    // ERROR: directorio no existe
    if (!dir.exists() || !dir.isDirectory) {
      println(s"Error: entities directory '$entitiesDir' not found")
      return List()
    }

    val people = loadFromFile(s"$entitiesDir/people.txt", "Person")
    if (people.isEmpty)
      println(s"Warning: Could not load $entitiesDir/people.txt")

    val universities = loadFromFile(s"$entitiesDir/universities.txt", "University")
    if (universities.isEmpty)
      println(s"Warning: Could not load $entitiesDir/universities.txt")

    val languages = loadFromFile(s"$entitiesDir/languages.txt", "ProgrammingLanguage")
    if (languages.isEmpty)
      println(s"Warning: Could not load $entitiesDir/languages.txt")

    val organizations = loadFromFile(s"$entitiesDir/organizations.txt", "Organization")
    if (organizations.isEmpty)
      println(s"Warning: Could not load $entitiesDir/organizations.txt")

    val places = loadFromFile(s"$entitiesDir/places.txt", "Place")
    if (places.isEmpty)
      println(s"Warning: Could not load $entitiesDir/places.txt")

    people.getOrElse(List()) :::
      universities.getOrElse(List()) :::
      languages.getOrElse(List()) :::
      organizations.getOrElse(List()) :::
      places.getOrElse(List())
  }
}
