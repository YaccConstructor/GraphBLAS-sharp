module GraphBLAS.FSharp.Tests.Backend.Algorithms.MSBFS

open Expecto
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Tests.Backend.QuickGraph.Algorithms
open GraphBLAS.FSharp.Tests.Backend.QuickGraph.CreateGraph
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions

let config = Utils.undirectedAlgoConfig
let workGroupSize = Utils.defaultWorkGroupSize

let makeLevelsTest context queue bfs (matrix: int [,]) =
    let graph = undirectedFromArray2D matrix 0

    let largestComponent =
        ConnectedComponents.largestComponent graph

    if largestComponent.Length > 1 then
        let sourceVertexCount = max 2 (largestComponent.Length / 10)

        let source =
            largestComponent.[0..sourceVertexCount]
            |> Array.toList

        let matrixHost =
            Utils.createMatrixFromArray2D CSR matrix ((=) 0)

        let matrixDevice = matrixHost.ToDevice context

        let expectedArray2D: int [,] =
            Array2D.zeroCreate sourceVertexCount (Array2D.length2 matrix)

        source
        |> Seq.iteri
            (fun i vertex ->
                (snd (BFS.runUndirected graph vertex))
                |> Utils.createArrayFromDictionary (Array2D.length1 matrix) 0
                |> Array.iteri (fun col value -> expectedArray2D.[i, col] <- value))

        let expected =
            Utils.createMatrixFromArray2D COO expectedArray2D ((=) 0)

        let actual: ClMatrix<int> = bfs queue matrixDevice source
        let actual = actual.ToHostAndFree queue

        matrixDevice.Dispose queue

        match actual, expected with
        | Matrix.COO a, Matrix.COO e -> Utils.compareCOOMatrix (=) a e
        | _ -> failwith "Not implemented"

let createLevelsTest context queue testFun =
    testFun
    |> makeLevelsTest context queue
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let levelsTestFixtures (testContext: TestContext) =
    [ let context = testContext.ClContext
      let queue = testContext.Queue

      let bfsLevels =
          Algorithms.MSBFS.runLevels
              (fst ArithmeticOperations.intAdd)
              (fst ArithmeticOperations.intMul)
              context
              workGroupSize

      createLevelsTest context queue bfsLevels ]

let levelsTests =
    TestCases.gpuTests "MSBFS Levels tests" levelsTestFixtures

let makeParentsTest context queue bfs (matrix: int [,]) =
    let graph = undirectedFromArray2D matrix -1

    let largestComponent =
        ConnectedComponents.largestComponent graph

    if largestComponent.Length > 1 then
        let sourceVertexCount = max 2 (largestComponent.Length / 10)

        let source =
            largestComponent.[0..sourceVertexCount]
            |> Array.toList

        let matrixHost =
            Utils.createMatrixFromArray2D CSR matrix ((=) -1)

        let matrixDevice = matrixHost.ToDevice context

        let expectedArray2D =
            HostPrimitives.MSBFSParents matrix source

        let expected =
            Utils.createMatrixFromArray2D COO expectedArray2D ((=) -1)

        let actual: ClMatrix<int> = bfs queue matrixDevice source
        let actual = actual.ToHostAndFree queue

        matrixDevice.Dispose queue

        match actual, expected with
        | Matrix.COO a, Matrix.COO e -> Utils.compareCOOMatrix (=) a e
        | _ -> failwith "Not implemented"

let createParentsTest context queue testFun =
    testFun
    |> makeLevelsTest context queue
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let parentsTestFixtures (testContext: TestContext) =
    [ let context = testContext.ClContext
      let queue = testContext.Queue

      let bfsLevels =
          Algorithms.MSBFS.runParents context workGroupSize

      createLevelsTest context queue bfsLevels ]

let parentsTests =
    TestCases.gpuTests "MSBFS Levels tests" parentsTestFixtures
