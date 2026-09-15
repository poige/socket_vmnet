#ifndef SOCKET_VMNET_CLI_H
#define SOCKET_VMNET_CLI_H

#include <uuid/uuid.h>

#include <vmnet/vmnet.h>

struct cli_options {
  // --socket-group
  char *socket_group;
  // --vmnet-mode, corresponds to vmnet_operation_mode_key
  operating_modes_t vmnet_mode;
  // --vmnet-interface, corresponds to vmnet_shared_interface_name_key
  char *vmnet_interface;
  // --vmnet-gateway, corresponds to vmnet_start_address_key
  char *vmnet_gateway;
  // --vmnet-dhcp-end, corresponds to vmnet_end_address_key
  char *vmnet_dhcp_end;
  // --vmnet-mask, corresponds to vmnet_subnet_mask_key
  char *vmnet_mask;
  // --vmnet-interface-id, corresponds to vmnet_interface_id_key
  uuid_t vmnet_interface_id;
  // --vmnet-network-identifier, corresponds to vmnet_network_identifier_key
  uuid_t vmnet_network_identifier;
  // --vmnet-nat66-prefix, corresponds to vmnet_nat66_prefix_key
  char *vmnet_nat66_prefix;
  // --vmnet-disable-dhcp; disables the vmnet DHCP server (requires macOS 26)
  bool vmnet_disable_dhcp;
  // -p, --pidfile; writes pidfile using permissions of socket_vmnet
  char *pidfile;
  // --sockbuf-size=BYTES; SO_SNDBUF/SO_RCVBUF applied to every accepted
  // client connection. -1 (default, unset) means "use the daemon's own
  // built-in default"; 0 means "leave the OS default untouched"; a
  // positive value is used as-is.
  int sockbuf_size;
  // --delivery-batch-size=BYTES; how much a delivery thread may coalesce into
  // one write() to a target. Distinct from --sockbuf-size, which sizes the
  // kernel socket buffer: one governs syscall amortization and head-of-line
  // blocking, the other how much the kernel will hold. They were a single
  // knob originally, which made both untunable. -1 (default, unset) means
  // "use the daemon's own built-in default".
  int delivery_batch_size;
  // --busy-poll=USEC; how long a delivery thread spins watching for new
  // published data before parking on the condvar. Named after Linux's
  // net.core.busy_poll. 0 (the default) parks immediately.
  int busy_poll_usec;
  // --skip-vmnet-write; DIAGNOSTIC ONLY. Suppresses every vmnet_write(),
  // measuring what the XPC round trip into vmnet.framework costs. Breaks all
  // external connectivity (DHCP, host access, internet) -- only VM<->VM
  // traffic on one daemon still works, because that is delivered by the local
  // flood rather than by vmnet. Never for real use.
#ifdef SOCKET_VMNET_DIAG
  bool skip_vmnet_write;
#endif
  // arg
  char *socket_path;
};

struct cli_options *cli_options_parse(int argc, char *argv[]);
void cli_options_destroy(struct cli_options *);

#endif /* SOCKET_VMNET_CLI_H */
