package com.moveinsync.scheduler.api;

import com.moveinsync.scheduler.core.*;
import com.moveinsync.scheduler.db.SchedulerStore;
import com.moveinsync.scheduler.model.Run;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api")
public class ApiController {
    private final JdbcTemplate jdbc; private final SchedulerStore store; private final PasswordEncoder encoder; private final int shards;
    public ApiController(JdbcTemplate jdbc,SchedulerStore store,PasswordEncoder encoder,@Value("${app.shards}") int shards) {
        this.jdbc=jdbc; this.store=store; this.encoder=encoder; this.shards=shards;
    }
    @GetMapping("/me")
public Map<String, String> me(Authentication auth) {
    String role = auth.getAuthorities().stream()
            .findFirst()
            .orElseThrow()
            .getAuthority()
            .replace("ROLE_", "");

    return Map.of(
            "username", auth.getName(),
            "role", role
    );
}

@GetMapping("/tenants")
public List<Map<String, Object>> tenants(Authentication auth) {
    if (admin(auth)) {
        return jdbc.queryForList(
                "SELECT id, name, zone, concurrency_cap AS \"concurrencyCap\", paused FROM tenant ORDER BY name"
        );
    }

    return jdbc.queryForList(
            "SELECT t.id, t.name, t.zone, t.concurrency_cap AS \"concurrencyCap\", t.paused " +
            "FROM tenant t JOIN app_user u ON u.tenant_id = t.id " +
            "WHERE u.username = ? ORDER BY t.name",
            auth.getName()
    );
}
    public record TenantRequest(@NotBlank String name,@NotBlank String zone,@Min(1) @Max(128) int concurrencyCap) {}
    public record UserRequest(@NotBlank String username,@NotBlank @Size(min=12) String password,@NotBlank @Pattern(regexp="ADMIN|OPERATOR|EMPLOYEE") String role,UUID tenantId) {}
    public record JobRequest(@NotBlank String kind,String cron,Instant at,@NotBlank String payload,int priority,
                             @Min(1) @Max(3600) int maxRunSeconds,@NotBlank @Pattern(regexp="FIRE_NOW|SKIP|COALESCE") String misfire) {}

    private boolean admin(Authentication auth) { return auth.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN")); }
    private boolean operator(Authentication auth) { return admin(auth) || auth.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_OPERATOR")); }
    private void access(Authentication auth,UUID tenant,boolean write) {
        if (admin(auth)) return;
        if (write && !operator(auth)) throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Operator role required");
        List<UUID> own = jdbc.query("SELECT tenant_id FROM app_user WHERE username=?",(r,n)->r.getObject(1,UUID.class),auth.getName());
        if (own.isEmpty() || !tenant.equals(own.getFirst())) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Resource not found");
    }
    private void exists(UUID tenant) {
        if (jdbc.queryForObject("SELECT count(*) FROM tenant WHERE id=?",Long.class,tenant)==0) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Resource not found");
    }
    @PostMapping("/admin/tenants") public Map<String,Object> tenant(@Valid @RequestBody TenantRequest request) {
        ZoneId.of(request.zone()); UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO tenant(id,name,zone,concurrency_cap) VALUES (?,?,?,?)",id,request.name(),request.zone(),request.concurrencyCap());
        for(int slot=0;slot<request.concurrencyCap();slot++) jdbc.update("INSERT INTO tenant_slot(tenant_id,slot) VALUES (?,?)",id,slot);
        return Map.of("id",id);
    }
    @PostMapping("/admin/users") public Map<String,String> user(@Valid @RequestBody UserRequest request) {
        if (!request.role().equals("ADMIN") && request.tenantId()==null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Tenant required");
        if(request.tenantId()!=null) exists(request.tenantId());
        jdbc.update("INSERT INTO app_user(username,password_hash,role,tenant_id) VALUES (?,?,?,?)",request.username(),encoder.encode(request.password()),request.role(),request.tenantId());
        return Map.of("username",request.username());
    }
    @PostMapping("/tenants/{tenant}/jobs") public Map<String,Object> createJob(Authentication auth,@PathVariable UUID tenant,@Valid @RequestBody JobRequest request) {
        access(auth,tenant,true); exists(tenant);
        if (!Set.of("ONE_OFF","CRON").contains(request.kind())) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid kind");
        String zoneName=jdbc.queryForObject("SELECT zone FROM tenant WHERE id=?",String.class,tenant);
        Instant at;
        if(request.kind().equals("ONE_OFF")) {
            if(request.at()==null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"at is required");
            at=request.at();
        } else {
            if(request.cron()==null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"cron is required");
            at=FireTimes.next(request.cron(),ZoneId.of(zoneName),store.dbNow());
        }
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO job(id,tenant_id,kind,cron,next_fire_at,payload,priority,max_run_seconds,misfire,shard) VALUES (?,?,?,?,?,?,?,?,?,?)",
            id,tenant,request.kind(),request.cron(),Timestamp.from(at),request.payload(),request.priority(),request.maxRunSeconds(),request.misfire(),ShardAssignment.shard(id,shards));
        return Map.of("id",id,"nextFireAt",at);
    }
    @GetMapping("/tenants/{tenant}/jobs/{job}/next") public List<Instant> next(Authentication auth,@PathVariable UUID tenant,@PathVariable UUID job,@RequestParam(defaultValue="5") int count) {
        access(auth,tenant,false);
        List<Map<String,Object>> rows=jdbc.queryForList("SELECT kind,cron,next_fire_at FROM job WHERE tenant_id=? AND id=?",tenant,job);
        if(rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Resource not found");
        Map<String,Object> row=rows.getFirst(); Instant first=((Timestamp)row.get("next_fire_at")).toInstant();
        if(row.get("kind").equals("ONE_OFF")) return List.of(first);
        if(count<1 || count>100) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"count must be 1..100");
        String zone=jdbc.queryForObject("SELECT zone FROM tenant WHERE id=?",String.class,tenant);
        List<Instant> result=new ArrayList<>();result.add(first);
        if(count>1) result.addAll(FireTimes.nextN((String)row.get("cron"),ZoneId.of(zone),first,count-1));return result;
    }
    @GetMapping("/tenants/{tenant}/overview")
public Map<String, Long> overview(Authentication auth, @PathVariable UUID tenant) {
    access(auth, tenant, false);

    return Map.of(
        "jobs", jdbc.queryForObject("SELECT count(*) FROM job WHERE tenant_id=?", Long.class, tenant),
        "running", jdbc.queryForObject("SELECT count(*) FROM job_run WHERE tenant_id=? AND state='RUNNING'", Long.class, tenant),
        "retry", jdbc.queryForObject("SELECT count(*) FROM job_run WHERE tenant_id=? AND state='RETRY'", Long.class, tenant),
        "dead", jdbc.queryForObject("SELECT count(*) FROM job_run WHERE tenant_id=? AND state='DEAD'", Long.class, tenant),
        "ready", jdbc.queryForObject("SELECT count(*) FROM job_run WHERE tenant_id=? AND state='READY'", Long.class, tenant),
        "nodes", (long) store.activeNodes().size()
    );
}

@GetMapping("/tenants/{tenant}/jobs")
public List<com.moveinsync.scheduler.model.Job> jobs(
        Authentication auth, @PathVariable UUID tenant) {
    access(auth, tenant, false);

    return jdbc.query(
        "SELECT * FROM job WHERE tenant_id=? ORDER BY next_fire_at",
        (rs, rowNum) -> new com.moveinsync.scheduler.model.Job(
            rs.getObject("id", UUID.class),
            rs.getObject("tenant_id", UUID.class),
            rs.getString("kind"),
            rs.getString("cron"),
            rs.getTimestamp("next_fire_at").toInstant(),
            rs.getString("payload"),
            rs.getInt("priority"),
            rs.getInt("max_run_seconds"),
            rs.getString("misfire"),
            rs.getInt("shard"),
            rs.getBoolean("enabled")
        ),
        tenant
    );
}

@GetMapping("/tenants/{tenant}/runs")
public List<Run> runs(Authentication auth, @PathVariable UUID tenant) {
    access(auth, tenant, false);

    return jdbc.query(
        "SELECT * FROM job_run WHERE tenant_id=? ORDER BY scheduled_at DESC LIMIT 100",
        (rs, rowNum) -> new Run(
            rs.getObject("id", UUID.class),
            rs.getObject("job_id", UUID.class),
            rs.getObject("tenant_id", UUID.class),
            rs.getTimestamp("scheduled_at").toInstant(),
            rs.getString("state"),
            rs.getInt("attempt"),
            rs.getTimestamp("available_at").toInstant(),
            rs.getTimestamp("lease_until") == null ? null : rs.getTimestamp("lease_until").toInstant(),
            rs.getString("owner"),
            rs.getLong("fence"),
            rs.getString("idempotency_key")
        ),
        tenant
    );
}
    @PostMapping("/tenants/{tenant}/jobs/{job}/trigger") public Map<String,Object> trigger(Authentication auth,@PathVariable UUID tenant,@PathVariable UUID job) {
        access(auth,tenant,true);
        if(jdbc.queryForObject("SELECT count(*) FROM job WHERE id=? AND tenant_id=?",Long.class,job,tenant)==0) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Resource not found");
        UUID id=UUID.randomUUID();Instant at=store.dbNow();
        jdbc.update("INSERT INTO job_run(id,job_id,tenant_id,scheduled_at,state,available_at,idempotency_key) VALUES (?,?,?,?,'READY',clock_timestamp(),?)",id,job,tenant,Timestamp.from(at),"manual:"+id);
        return Map.of("runId",id);
    }
    @PostMapping("/tenants/{tenant}/pause") public void pause(Authentication auth,@PathVariable UUID tenant) { access(auth,tenant,true);jdbc.update("UPDATE tenant SET paused=true WHERE id=?",tenant); }
    @PostMapping("/tenants/{tenant}/resume") public void resume(Authentication auth,@PathVariable UUID tenant) { access(auth,tenant,true);jdbc.update("UPDATE tenant SET paused=false WHERE id=?",tenant); }
    @GetMapping("/tenants/{tenant}/dead") public List<Run> dead(Authentication auth,@PathVariable UUID tenant) { access(auth,tenant,true);return store.dead(tenant); }
    @PostMapping("/tenants/{tenant}/dead/{run}/replay") public void replay(Authentication auth,@PathVariable UUID tenant,@PathVariable UUID run) {
        access(auth,tenant,true);if(!store.replay(tenant,run)) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Resource not found");
    }
    @GetMapping("/tenants/{tenant}/runs/{run}") public Run run(Authentication auth,@PathVariable UUID tenant,@PathVariable UUID run) {
        access(auth,tenant,false);return store.run(run).filter(r->r.tenantId().equals(tenant)).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Resource not found"));
    }
    @GetMapping("/tenants/{tenant}/runs/{run}/effects") public Map<String,Long> effects(Authentication auth,@PathVariable UUID tenant,@PathVariable UUID run) {
        this.run(auth,tenant,run);return Map.of("count",store.effectCount(run));
    }
}
