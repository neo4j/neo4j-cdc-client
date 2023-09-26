# CDC Client Library for Neo4j

The project provides a CDC client library to be used among source connectors built by Neo4j.

## Adding the package as a dependency

Currently, this library is deployed to Github Packages, so you need to configure your maven repositories accordingly.

* Add dependency

```xml

<dependency>
    <groupId>org.neo4j.connectors</groupId>
    <artifactId>cdc</artifactId>
    <version>${cdc.version}</version>
</dependency>
```

* Add repository

```xml

<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/neo4j/neo4j-cdc-client</url>
    </repository>
</repositories>
```

* Add access credentials
  Create a personal access token in Github and give it a permission to read packages.
  Then add the following content into `settings/servers` inside ~/.m2/settings.xml.

```xml

<server>
    <id>github</id>
    <username>your-github-user-name</username>
    <password>your-personal-access-token</password>
</server>
```

## Usage

In order to use the client, create an instance of `org.neo4j.cdc.client.CDCClient` class, providing an instance of a
driver instance, an optional polling interval to mimic streaming behaviour with under the hood polling, and optional
list
of selectors that you can specify what entities' changes you are interested.

A typical use case would be;

* A simple use case with polling

```java
import org.neo4j.cdc.client.CDCClient;
import org.neo4j.cdc.client.model.ChangeIdentifier;

class Main {
    public static void main() {
        var driver = GraphDatabase.driver("neo4j://localhost");
        var client = new CDCClient(driver);

        // grab a change identifier to listen changes from.
        // could be retrieved by CDCClient#earliest or CDCClient#current methods.
        var from = new ChangeIdentifier("<some change identifier>");

        client.query(from)
                .doOnNext(e -> System.out.println(e)) // do something with the received change
                .blockLast(); // just wait until the query terminates
    }
}
```

* A simple use case with streaming

```java
import java.time.Duration;

import org.neo4j.cdc.client.CDCClient;
import org.neo4j.cdc.client.model.ChangeIdentifier;

class Main {
    public static void main() {
        var driver = GraphDatabase.driver("neo4j://localhost");
        var client = new CDCClient(driver);

        // grab a change identifier to listen changes from.
        // could be retrieved by CDCClient#earliest or CDCClient#current methods.
        var from = new ChangeIdentifier("<some change identifier>");

        client.stream(from)
                .doOnNext(e -> System.out.println(e)) // do something with the received change
                .timeout(Duration.ofMinutes(1)); // run for 1 minute
    }
}
```

* A simple use case with selectors

```java
import static java.util.Collections.emptySet;

import java.time.Duration;
import java.util.Set;

import org.neo4j.cdc.client.CDCClient;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import org.neo4j.cdc.client.model.EntityOperation;
import org.neo4j.cdc.client.selector.EntitySelector;
import org.neo4j.cdc.client.selector.NodeSelector;
import org.neo4j.cdc.client.selector.RelationshipNodeSelector;
import org.neo4j.cdc.client.selector.RelationshipSelector;
import org.neo4j.driver.GraphDatabase;

class Main {
    public static void main() {
        var driver = GraphDatabase.driver("neo4j://localhost");
        var client = new CDCClient(
                driver,
                new EntitySelector(EntityOperation.DELETE), // any delete operation on nodes or relationships
                new NodeSelector(null, emptySet(), Set.of("Person")), // any operation on nodes with Person label
                new NodeSelector(null, emptySet(), Set.of("Company")), // any operation on nodes with Company label
                new RelationshipSelector(
                        null,
                        emptySet(),
                        "WORKS_FOR",
                        new RelationshipNodeSelector(Set.of("Person")),
                        new RelationshipNodeSelector(
                                Set.of("Company")))); // any operation on relationships of type WORKS_FOR between nodes of `Person` and `Company` labels

        // grab a change identifier to listen changes from.
        // could be retrieved by CDCClient#earliest or CDCClient#current methods.
        var from = new ChangeIdentifier("<some change identifier>");

        client.stream(from)
                .doOnNext(e -> System.out.println(e)) // do something with the received change
                .timeout(Duration.ofMinutes(1)); // run for 1 minute
    }
}
```

### Patterns

There is also a pattern syntax that could be used as a means to build selectors.
They look like ordinary Cypher entity patterns with a couple of additional features.

* `(:Person)` -> select nodes of `Person` label
* `(:Person:Employee)` -> select nodes of both `Person` and `Employee` label
* `(:Person)-[:WORKS_FOR]->(:Company)` -> select relationships of type `WORKS_FOR` between `Person` (start)
  and `Company` (end) nodes.
* `(:Person)-[:WORKS_FOR]-(:Company)` -> select relationships of type `WORKS_FOR` between `Person` and `Company` nodes,
  in
  either direction.
* `(:Person{*,-name})` -> select nodes of `Person` label, exclude `name` property from the result.
* `(:Person{+id, +name})` -> select nodes of `Person` label, only include `id` and `name` properties in the result.

You can convert patterns into selectors as follows;

```java
import java.util.List;

import org.neo4j.cdc.client.CDCClient;
import org.neo4j.cdc.client.pattern.Pattern;

class Main {
    public static void main() {
        var driver = GraphDatabase.driver("neo4j://localhost");
        var selectors = List.of("(:Person{*})", "(:Company{*})", "(:Person)-[:WORKS_FOR{*}]->(:Company)").stream()
                .flatMap(Pattern::parse)
                .flatMap(Pattern::toSelector)
                .toList();
        var client = new CDCClient(driver, selectors);
    }
}
```

## Development & Contributions

### Internal Maven Repository

For the build to succeed, it needs the package [build-resources](https://github.com/neo4j/connectors-build-resources)
that is being published to Github Packages.
In order to access it, create a personal access token in Github and give it a permission to read packages.
Then add the following content into `settings/servers` inside ~/.m2/settings.xml.

```xml

<server>
    <id>github</id>
    <username>your-github-user-name</username>
    <password>your-personal-access-token</password>
</server>
```

### Build locally

You can build and package the project using;

```
mvn clean verify
```

You'll find the client library at `target/cdc-{version}.jar`.

### Code Format

For Java code, we follow the Palantir Java format code style, enforced by spotless plugin.
Remember that your builds will fail if your changes doesn't match the enforced code style, but you can
use `./mvnw spotless:apply` to format your code.

For POM files, we are using [sortpom](https://github.com/Ekryd/sortpom) to have a tidier project object model. Remember
that your builds will fail if your changes doesn't conform to the enforced rules, but you can use `./mvnw sortpom:sort`
to format it accordingly.
