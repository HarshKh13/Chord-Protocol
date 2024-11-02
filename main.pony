use "math"
use "collections"

actor Node
    var env: Env
    var id: F64
    var numNodes: F64
    var fingerSize: F64
    var base: F64 = 2.0

    new create(env': Env, id': F64, numNodes': F64, fingerSize': F64) =>
        env = env'
        id = id'
        numNodes = numNodes'
        fingerSize = fingerSize'
    
    be receive_request(key: F64, coordinator: Coordinator) =>
        if(key == id) then
            coordinator.foundNode()
        else 
            closest_preceding_node(key, coordinator)
        end

    be closest_preceding_node(key: F64, coordinator: Coordinator) =>
        var closestPrecedingNodeId: F64 = 0
        if(id > key) then
            var lastFinger: F64 = (id+(base.pow(fingerSize)))%numNodes
            if(lastFinger > id) then
                closestPrecedingNodeId = lastFinger
            else 
                var i: F64 = fingerSize
                while i>=0 do
                    var finger: F64 = (id+(base.pow(i)))%numNodes 
                    if finger <= key then
                        closestPrecedingNodeId = finger
                        break
                    end
                    i = i-1
                end
            end
        else  
            var i: F64 = fingerSize
            while i>=0 do
                var finger: F64 = (id+(base.pow(i)))%numNodes 
                if ((finger <= key) and (finger > id)) then
                    closestPrecedingNodeId = finger
                    break
                end
                i = i-1
            end
        end 
        coordinator.send_request_to_closest_preceding_node(key, closestPrecedingNodeId, coordinator)


actor Coordinator
    var env: Env
    var nodesList: Array[Node] = Array[Node]
    var numNodes: F64
    var numRequests: F64
    var fingerSize: F64
    var numTerminatedRequests: F64 
    var totalHops: F64

    new create(env': Env, numNodes': F64, fingerSize': F64, numRequests': F64) =>
        env = env'
        numNodes = numNodes'
        fingerSize =  fingerSize'
        numRequests = numRequests'
        numTerminatedRequests = 0
        totalHops = 0

        for i in Range[F64](0,numNodes) do
            let node: Node = Node(env,i,numNodes,fingerSize)
            nodesList.push(node)
        end
    
    be send_request_to_closest_preceding_node(key: F64, closestPrecedingNodeId: F64, coordinator: Coordinator) =>
        try
            nodesList(closestPrecedingNodeId.u64().usize())?.receive_request(key, coordinator)
            totalHops = totalHops + 1
        else 
            env.out.print("Some Error Occured 8!")
        end
    
    be foundNode() =>
        numTerminatedRequests = numTerminatedRequests + 1
        if(numTerminatedRequests == (numRequests*numNodes)) then
            var averageHops: F64 = totalHops/numTerminatedRequests
            env.out.print(averageHops.string())
        end

actor Main
    new create(env: Env) =>
        try
            var numNodesString: String = env.args(1)?
            var numRequestString: String = env.args(2)?

            var numNodes: F64 = numNodesString.f64()?
            var numRequests: F64 = numRequestString.f64()?

            var logVal: F64 = numNodes.log2()
            var roundedLogVal: U64 = logVal.u64()
            var fingerSize: F64 = roundedLogVal.f64()
            var base: F64 = 2.0

            var coordinator: Coordinator = Coordinator(env, numNodes, fingerSize, numRequests)
            for id in Range[F64](0, numNodes) do
                for j in Range[F64](0, numRequests) do
                    var key: F64 = j%numNodes
                    coordinator.send_request_to_closest_preceding_node(key,id,coordinator)
                end
            end

        else 
            env.out.print("Some Error Occured 10!")
        end