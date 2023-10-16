module GraphBLAS.FSharp.Tests.Backend.Algorithms.BFS

open Expecto
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.Backend.QuickGraph.Algorithms
open GraphBLAS.FSharp.Tests.Backend.QuickGraph.CreateGraph
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Objects

let testFixtures (testContext: TestContext) =
    [ let config = Utils.undirectedAlgoConfig
      let context = testContext.ClContext
      let queue = testContext.Queue
      let workGroupSize = Utils.defaultWorkGroupSize

      let testName =
          sprintf "Test on %A" testContext.ClContext

      let bfs =
          Algorithms.BFS.singleSource
              ArithmeticOperations.intSumOption
              ArithmeticOperations.intMulOption
              context
              workGroupSize

      let bfsSparse =
          Algorithms.BFS.singleSourceSparse
              ArithmeticOperations.intSumOption
              ArithmeticOperations.intMulOption
              context
              workGroupSize

      let bfsPushPull =
          Algorithms.BFS.singleSourcePushPull
              ArithmeticOperations.intSumOption
              ArithmeticOperations.intMulOption
              context
              workGroupSize

      testPropertyWithConfig config testName
      <| fun (matrix: int [,]) ->

          let graph = undirectedFromArray2D matrix 0

          let largestComponent =
              ConnectedComponents.largestComponent graph

          if largestComponent.Length > 0 then
              let source = largestComponent.[0]

              let expected =
                  (snd (BFS.runUndirected graph source))
                  |> Utils.createArrayFromDictionary (Array2D.length1 matrix) 0

              let matrixHost =
                  Utils.createMatrixFromArray2D CSR matrix ((=) 0)

              let matrix = matrixHost.ToDevice context

              let res = bfs queue matrix source

              let resSparse = bfsSparse queue matrix source

              let resPushPull = bfsPushPull queue matrix source

              let resHost = res.ToHost queue
              let resHostSparse = resSparse.ToHost queue
              let resHostPushPull = resPushPull.ToHost queue

              matrix.Dispose queue
              res.Dispose queue
              resSparse.Dispose queue
              resPushPull.Dispose queue

              match resHost, resHostSparse, resHostPushPull with
              | Vector.Dense resHost, Vector.Dense resHostSparse, Vector.Dense resHostPushPull ->
                  let actual = resHost |> Utils.unwrapOptionArray 0

                  let actualSparse =
                      resHostSparse |> Utils.unwrapOptionArray 0

                  let actualPushPull =
                      resHostPushPull |> Utils.unwrapOptionArray 0

                  Expect.sequenceEqual actual expected "Dense bfs is not as expected"
                  Expect.sequenceEqual actualSparse expected "Sparse bfs is not as expected"
                  Expect.sequenceEqual actualPushPull expected "Push-pull bfs is not as expected"
              | _ -> failwith "Not implemented" ]

let tests =
    TestCases.gpuTests "Bfs tests" testFixtures
