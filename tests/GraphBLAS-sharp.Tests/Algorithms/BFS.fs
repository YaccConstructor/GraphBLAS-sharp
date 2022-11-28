module Algo.BFS

open Expecto
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.QuickGraph.Algorithms
open GraphBLAS.FSharp.Tests.QuickGraph.CreateGraph

let testFixtures (testContext: TestContext) =
    [ let config = Utils.undirectedAlgoConfig
      let context = testContext.ClContext
      let queue = testContext.Queue
      let workGroupSize = 32

      let testName =
          sprintf "Test on %A" testContext.ClContext

      let bfs =
          Algorithms.BFS.singleSource
              context
              StandardOperations.intSum
              StandardOperations.intMul
              <@ (+) @>
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
                  Utils.createMatrixFromArray2D GraphBLAS.FSharp.CSR matrix (fun x -> x = 0)

              let matrix = matrixHost.ToBackend context

              match matrix with
              | MatrixCSR mtx ->
                  let res = bfs queue mtx source

                  let resHost = res.ToHost queue

                  (mtx :> IDeviceMemObject).Dispose queue
                  res.Dispose queue

                  match resHost with
                  | VectorDense resHost ->
                      let actual = resHost |> Utils.unwrapOptionArray 0

                      Expect.sequenceEqual actual expected "Sequences must be equal"
                  | _ -> failwith "Not implemented"
              | _ -> failwith "Not implemented" ]

let tests =
    TestCases.gpuTests "Bfs tests" testFixtures
