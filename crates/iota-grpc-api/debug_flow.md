- Follow the guide of https://docs.iota.org/developer/getting-started/local-network, to build and run a local IOTA network.
- Use the iota-grpc-api example to connect to the local network.
- Check if we can really get the subscribed events from the local network, especially the filtered events.
- Make sure the grpc api url is configured correctly in the example, by adding: `grpc-api-address: "{GRPC_ADDRESS}"` in the configuration file (e.g., fullnode.yaml).
- The steps to start the node with the correct configuration follow:```
   - RUST_LOG="off,iota_node=info" iota start --with-faucet --network.config=/tmp/l1grpc
   - Then edit the fullnode.yml
  - And start it again
- Previously we have a bug, but it should be fixed now. The following steps are used to debug the issue previously, we can use them to verify if the issue is resolved:
  - Start the node with the command: `RUST_LOG="off,iota_node=info" iota start --with-faucet --network.config=/tmp/l1grpc`
  - Edit the fullnode.yml file to ensure the grpc-api-address is set correctly.
  - Restart the node after editing the configuration file.
  - Subscribe to events with the filter I attached
  - Call grpcclient.Recv() and it basically halts there, waiting for messages to come in.
  - It seems like connecting and subscribing works, but receiving events does not. 
  - We tried this filter:
	events2, err := c.Client.QueryEvents(ctx, iotaclient.QueryEventsRequest{
		Query: &iotajsonrpc.EventFilter{MoveEventType: &iotago.StructTag{
			Address: &packageID,
			Module:  iscmove.RequestModuleName,
			Name:    iscmove.RequestEventObjectName,
		}}})
     - which is through the JSON-RPC endpoint and we get events
  - We use this filter for the WebSocket:
	err := w.client.SubscribeEvent(ctx, &iotajsonrpc.EventFilter{
		And: &iotajsonrpc.AndOrEventFilter{
			Filter1: &iotajsonrpc.EventFilter{MoveEventType: &iotago.StructTag{
				Address: &packageID,
				Module:  iscmove.RequestModuleName,
				Name:    iscmove.RequestEventObjectName,
			}},
			Filter2: &iotajsonrpc.EventFilter{MoveEventField: &iotajsonrpc.EventFilterMoveEventField{
				Path:  iscmove.RequestEventAnchorFieldName,
				Value: anchorID.String(),
			}},
		},
	}, events)
      - but using this MoveEvent Filter on QueryEvents gives an error, as the FullNode apparently doesn't support it, only the Indexer (and Websocket).