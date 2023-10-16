module GraphBLAS.FSharp.Tests.Backend.Algorithms.PageRank

open Expecto
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions

let testFixtures (testContext: TestContext) =
    [ let context = testContext.ClContext
      let queue = testContext.Queue
      let workGroupSize = Utils.defaultWorkGroupSize

      let testName =
          sprintf "Test on %A" testContext.ClContext

      let pageRank =
          Algorithms.PageRank.run context workGroupSize

      testCase testName
      <| fun () ->

          let matrix = Array2D.zeroCreate 4 4

          matrix.[0, 1] <- 1f
          matrix.[0, 2] <- 1f
          matrix.[0, 3] <- 1f
          matrix.[1, 2] <- 1f
          matrix.[1, 3] <- 1f
          matrix.[2, 0] <- 1f
          matrix.[3, 2] <- 1f
          matrix.[3, 0] <- 1f

          let matrixHost =
              Utils.createMatrixFromArray2D CSR matrix ((=) 0f)

          let matrix = matrixHost.ToDevice context

          let preparedMatrix =
              Algorithms.PageRank.prepareMatrix context workGroupSize queue matrix

          let res = pageRank queue preparedMatrix

          let resHost = res.ToHost queue

          preparedMatrix.Dispose queue
          matrix.Dispose queue
          res.Dispose queue

          let expected =
              [| 0.3681506515f
                 0.1418093443f
                 0.2879616022f
                 0.2020783126f |]

          match resHost with
          | Vector.Dense resHost ->
              let actual = resHost |> Utils.unwrapOptionArray 0f

              for i in 0 .. actual.Length - 1 do
                  Expect.isTrue (Utils.float32IsEqual actual.[i] expected.[i]) "Values should be equal"

          | _ -> failwith "Not implemented" ]

let tests =
    TestCases.gpuTests "Bfs tests" testFixtures
