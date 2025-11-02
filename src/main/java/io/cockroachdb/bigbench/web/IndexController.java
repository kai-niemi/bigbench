package io.cockroachdb.bigbench.web;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.hateoas.IanaLinkRelations;
import org.springframework.hateoas.Link;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import io.cockroachdb.bigbench.config.ProfileNames;
import io.cockroachdb.bigbench.jdbc.MetaDataUtils;
import io.cockroachdb.bigbench.util.RandomData;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo;
import static org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn;

@RestController
@Profile(ProfileNames.HTTP)
public class IndexController {
    @Autowired
    private DataSource dataSource;

    @GetMapping("/")
    public ResponseEntity<MessageModel> index() {
        MessageModel resource = MessageModel.from("Welcome to BigBench API");
        resource.setNotice(RandomData.randomRoachFact());
        resource.setCockroachVersion(MetaDataUtils.databaseVersion(dataSource));

        resource.add(linkTo(methodOn(getClass())
                .index())
                .withSelfRel());
        resource.add(Link.of("https://www.cockroachlabs.com/docs/stable/import-into")
                .withRel(IanaLinkRelations.CITE_AS)
                .withType(MediaType.TEXT_HTML_VALUE));

        resource.add(linkTo(methodOn(TableController.class)
                .getTable(null, null))
                .withRel(LinkRelations.TABLE_REL)
                .withTitle("Table schema"));

        MetaDataUtils.listSchemas(dataSource, rs -> {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");

                resource.add(linkTo(methodOn(TableController.class)
                        .index(schema))
                        .withRel(LinkRelations.SCHEMA_INDEX_REL)
                        .withTitle("Table schema"));

            }
        });

        resource.add(Link.of(ServletUriComponentsBuilder.fromCurrentContextPath()
                        .pathSegment("actuator")
                        .buildAndExpand()
                        .toUriString())
                .withRel(LinkRelations.ACTUATORS_REL)
                .withTitle("Spring boot actuators"));

        return ResponseEntity.ok(resource);
    }

    @GetMapping("/health")
    public ResponseEntity<MessageModel> health() {
        return ResponseEntity.ok().build();
    }
}
