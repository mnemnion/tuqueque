//! Zig module root file for tuqueque
const std = @import("std");

test "module mentioned" {
    std.debug.print("hello from tuqueque module\n", .{});
}
