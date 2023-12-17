module GraphBLAS.FSharp.Tests.Backend.Algorithms.PageRank

open Expecto
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Objects

let private accuracy = float32 Accuracy.low.absolute

let prepareNaive (matrix: float32 [,]) =
    let result = Array2D.copy matrix
    let rowCount = Array2D.length1 matrix
    let outDegrees = Array.zeroCreate rowCount

    //Count degree
    Array2D.iteri (fun r c v -> outDegrees.[r] <- outDegrees.[r] + (if v <> 0f then 1f else 0f)) matrix

    //Set value
    Array2D.iteri
        (fun r c v ->
            result.[r, c] <-
                if v <> 0f then
                    Backend.Algorithms.PageRank.alpha / outDegrees.[r]
                else
                    0f)
        matrix

    //Transpose
    Array2D.iteri
        (fun r c _ ->
            if r > c then
                let temp = result.[r, c]
                result.[r, c] <- result.[c, r]
                result.[c, r] <- temp)
        matrix

    result

let pageRankNaive (matrix: float32 [,]) =
    let rowCount = Array2D.length1 matrix
    let mutable result = Array.zeroCreate rowCount

    let mutable prev =
        Array.create rowCount (1f / (float32 rowCount))

    let mutable error = accuracy + 1f

    let addConst =
        (1f - Backend.Algorithms.PageRank.alpha)
        / (float32 rowCount)

    while (error > accuracy) do
        for r in 0 .. rowCount - 1 do
            result.[r] <- 0f

            for c in 0 .. rowCount - 1 do
                result.[r] <- result.[r] + matrix.[r, c] * prev.[c]

            result.[r] <- result.[r] + addConst

        error <-
            sqrt
            <| Array.fold2 (fun e x1 x2 -> e + (x1 - x2) * (x1 - x2)) 0f result prev

        let temp = result
        result <- prev
        prev <- temp

    prev

let testFixtures (testContext: TestContext) =
    [ let config = Utils.undirectedAlgoConfig
      let context = testContext.ClContext
      let queue = testContext.Queue
      let workGroupSize = Utils.defaultWorkGroupSize

      let testName =
          sprintf "Test on %A" testContext.ClContext

      let pageRank =
          Algorithms.PageRank.run context workGroupSize

      testPropertyWithConfig config testName
      <| fun (matrix: float32 [,]) ->
          let matrixHost =
              Utils.createMatrixFromArray2D CSR matrix ((=) 0f)

          if matrixHost.NNZ > 0 then
              let preparedMatrixExpected = prepareNaive matrix

              let expected = pageRankNaive preparedMatrixExpected

              let matrix = matrixHost.ToDevice context

              let preparedMatrix =
                  Algorithms.PageRank.prepareMatrix context workGroupSize queue matrix

              let res = pageRank queue preparedMatrix accuracy

              let resHost = res.ToHost queue

              preparedMatrix.Dispose queue
              matrix.Dispose queue
              res.Dispose queue

              match resHost with
              | Vector.Dense resHost ->
                  let actual = resHost |> Utils.unwrapOptionArray 0f

                  for i in 0 .. actual.Length - 1 do
                      Expect.isTrue
                          (Utils.float32IsEqualLowAccuracy actual.[i] expected.[i])
                          (sprintf "Values should be equal. Expected %A, actual %A" expected.[i] actual.[i])

              | _ -> failwith "Not implemented" ]

let tests =
    TestCases.gpuTests "PageRank tests" testFixtures
