module GraphBLAS.FSharp.Tests.Backend.Algorithms.SSSP

open Expecto
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.Backend.QuickGraph.Algorithms
open GraphBLAS.FSharp.Tests.Backend.QuickGraph.CreateGraph
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Objects

let testFixtures (testContext: TestContext) =
    [ let config = Utils.undirectedAlgoConfig
      let context = testContext.ClContext
      let queue = testContext.Queue
      let workGroupSize = Utils.defaultWorkGroupSize

      let testName =
          sprintf "Test on %A" testContext.ClContext

      let ssspDense =
          Algorithms.SSSP.run context workGroupSize

      testPropertyWithConfig config testName
      <| fun (matrix: int [,]) ->

          let matrix = Array2D.map (fun x -> abs x) matrix

          let graph = undirectedFromArray2D matrix 0

          let largestComponent =
              ConnectedComponents.largestComponent graph

          if largestComponent.Length > 0 then
              let source = largestComponent.[0]

              let expected =
                  SSSP.runUndirected matrix (directedFromArray2D matrix 0) source
                  |> Array.map
                      (fun x ->
                          match x with
                          | Some x -> Some(int x)
                          | None -> None)

              let matrixHost =
                  Utils.createMatrixFromArray2D CSR matrix ((=) 0)

              let matrix = matrixHost.ToDevice context

              match matrix with
              | ClMatrix.CSR mtx ->
                  let resDense = ssspDense queue mtx source

                  let resHost = resDense.ToHost queue

                  (mtx :> IDeviceMemObject).Dispose queue
                  resDense.Dispose queue

                  match resHost with
                  | Vector.Dense resHost ->
                      let actual = resHost

                      Expect.sequenceEqual actual expected "Sequences must be equal"
                  | _ -> failwith "Not implemented"
              | _ -> failwith "Not implemented" ]

let tests =
    TestCases.gpuTests "SSSP tests" testFixtures
