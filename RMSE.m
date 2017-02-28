clc
clear all
input_test = importdata ('possible dataset\test.csv');
%input_test(:,4  ) = [];
d = 5 ; %%%H0,W0
W =[];
H = [];
for  i =0:1:d-1
    Hname=strcat('possible dataset\HMatrix\H',num2str(i));
    Wname=strcat('possible dataset\WMatrix\W',num2str(i));
    tempH = importdata(Hname,'\t');
    tempW = importdata(Wname,'\t');
    W = [W;tempW];
    H = [H tempH'];
end

rmse = 0;
for i=1:1:length(input_test)
            predict =  sigmf(W( input_test(i,1), :  )*H(:,  input_test( i , 2)),[1,3])*4+1;
            %predict =  W( input_test(i,1), :  )*H(:,  input_test( i , 2));
           %predict = 3.5;
                   if  predict> 5
                       predict = 5;
                   elseif predict<1
                       predict = 1;
                   end 
           rmse  = rmse + (input_test(i, 3) -  predict   )^2 ;
end

rmse = sqrt(  rmse/length(input_test)    )