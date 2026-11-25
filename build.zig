// Build script for tuqueque
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    const opts = b.addOptions();
    opts.addOption(?usize, "custom_alignment", b.option(
        usize,
        "custom_alignment",
        "Use a custom amount of padding to isolate atomic variables",
    ));

    const tuqueque_mod = b.addModule("tuqueque", .{
        .root_source_file = b.path("src/tuqueque.zig"),
        .target = target,
        .optimize = optimize,
    });
    tuqueque_mod.addOptions("options", opts);

    const test_filters = b.option(
        []const []const u8,
        "test-filter",
        "Skip tests that do not match any filter",
    ) orelse &[0][]const u8{};

    const tuqueque_test_mod = b.createModule(.{
        .root_source_file = b.path("src/tuqueque.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = true,
    });
    tuqueque_test_mod.addOptions("options", opts);

    const module_unit_tests = b.addTest(.{
        .root_module = tuqueque_test_mod,
        .filters = test_filters,
    });

    const run_module_unit_tests = b.addRunArtifact(module_unit_tests);
    run_module_unit_tests.has_side_effects = false;

    const test_step = b.step("test", "Run unit tests");

    test_step.dependOn(&run_module_unit_tests.step);

    const run_kcov = b.addSystemCommand(&.{
        "kcov",
        "--clean",
        "--exclude-line=unreachable,expect(false),@panic,kcov-defer-error,kcov-test-cleanup,kcov-miss",
    });
    run_kcov.setCwd(b.path("."));
    run_kcov.addDirectoryArg2(b.path("src"), .{ .prefix = "--include-pattern=", .make_absolute = true });
    const coverage_output = run_kcov.addOutputDirectoryArg(".");
    run_kcov.addArtifactArg(module_unit_tests);

    run_kcov.enableTestRunnerMode();

    const install_coverage = b.addInstallDirectory(.{
        .source_dir = coverage_output,
        .install_dir = .{ .custom = "coverage" },
        .install_subdir = "",
    });

    const coverage_step = b.step("coverage", "Generate coverage (kcov must be installed)");
    coverage_step.dependOn(&install_coverage.step);
}
