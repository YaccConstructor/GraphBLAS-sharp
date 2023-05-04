namespace GraphBLAS.FSharp.Backend.Algorithms

open GraphBLAS.FSharp.Backend
open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Backend.Vector.Dense
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClCell

module SSSP =
    let run (clContext: ClContext) workGroupSize =

        let less = ArithmeticOperations.less<int>
        let min = ArithmeticOperations.min<int>
        let plus = ArithmeticOperations.intSumExplicit

        let spMVTo =
            SpMV.runTo clContext min plus workGroupSize

        let create = ClArray.create clContext workGroupSize

        let createMask = ClArray.create clContext workGroupSize

        let ofList = Vector.ofList clContext workGroupSize

        let eWiseMulLess =
            ClArray.map2Inplace clContext workGroupSize less

        let eWiseAddMin =
            ClArray.map2Inplace clContext workGroupSize min

        let fillSubVectorTo =
            DenseVector.assignByMaskInplace clContext (Convert.assignToOption Mask.assignComplemented) workGroupSize

        let countNNZ =
            ClArray.count clContext workGroupSize Predicates.isSome

        fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix.CSR<int>) (source: int) ->
            let vertexCount = matrix.RowCount

            //None is System.Int32.MaxValue
            let distance =
                ofList queue DeviceOnly Dense vertexCount [ source, 0 ]

            let mutable f1 =
                ofList queue DeviceOnly Dense vertexCount [ source, 0 ]

            let mutable f2 =
                create queue DeviceOnly vertexCount None
                |> ClVector.Dense

            let m =
                createMask queue DeviceOnly vertexCount None
                |> ClVector.Dense

            let mutable stop = false

            while not stop do
                match f1, f2, distance, m with
                | ClVector.Dense front1, ClVector.Dense front2, ClVector.Dense dist, ClVector.Dense mask ->
                    //Getting new frontier
                    spMVTo queue matrix front1 front2

                    //Checking which distances were updated
                    eWiseMulLess queue front2 dist mask
                    //Updating
                    eWiseAddMin queue dist front2 dist

                    //Filtering unproductive vertices
                    fillSubVectorTo queue front2 mask (clContext.CreateClCell 0) front2

                    //Swap fronts
                    let temp = f1
                    f1 <- f2
                    f2 <- temp

                    //Checking if no distances were updated
                    stop <- (countNNZ queue mask) = 0

                | _ -> failwith "not implemented"

            f1.Dispose queue
            f2.Dispose queue
            m.Dispose queue

            distance
