#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(shiny)

library(leaflet)
library(dplyr)
library(leaflet.extras)
#import data

data<-as.data.frame(read.table(url('https://raw.githubusercontent.com/bpaszko/big-data-projekt/master/dashboard/lonlat.csv'),sep = ',',colClasses=c("numeric", "numeric"),na.strings='NULL'))
colnames(data)<-c('latitude','longitude')
mag_temp<-rep(3,179)
miejscowosci<-read.csv2(url('https://raw.githubusercontent.com/bpaszko/big-data-projekt/master/dashboard/miasta.csv'), header = F,encoding = 'UTF-8')
miejscowosci_temp<-miejscowosci[,1]
dat1<-data.frame(cbind(data, city=tolower(miejscowosci_temp)))
data<-cbind(data, mag=mag_temp, city=miejscowosci_temp)


ui <- fluidPage(
  titlePanel("Czy bedzie smog?"),
  sidebarLayout(
    
    sidebarPanel(
      selectInput("variable", "Data:",
                  ""),width = 2
    ),
    
  mainPanel( 
    #this will create a space for us to display our map
    leafletOutput(outputId = "mymap", height = "950px", width = "950px"), 

    width = 8
  ))
)

server <- function(input, output, session) {

  data1 =reactive({
    dt<-read.csv(url("https://storage.googleapis.com/smog_pred_bucket/smog_pred.csv"), encoding="UTF-8",sep = ',')
    #choices = sort(as.POSIXlt(as.POSIXct(as.character(unique(dt[,1])),format="%Y-%m-%d %H:%M:%S")))
    colnames(dt)=c('data','city','odczyt','wskaznik','datapob')
    index<-which(dt$wskaznik=="PM10")
    dt1<-dt[index,]
    dt2<-dt[-index,]
    dt<-merge(x=dt1[,1:4],y=dt2, by=c("data","city"), all.x = TRUE)
    colnames(dt)=c('data','city','odczyt','wskaznik',"odczyt2",'wskaznik2','datapob')
    dt$wskaznik2[is.na(dt$wskaznik2)] <- 'PM2.5'
    dt$odczyt2[is.na(dt$odczyt2)] <- '-'
    dt<-dt[,1:6]
    dt<-merge(dt,dat1)

    #dt<-merge(dt, data1)
    return (dt)
  })  


  pal2 <- colorFactor(
    palette = c('yellow', 'red'),
    domain = data$depth_type
  )
  
  #create the map
  
  output$mymap <- renderLeaflet({
    smog<-data1()
    leaflet(smog) %>% 
      setView(lng = 19, lat = 52, zoom = 7)  %>% 
      addTiles() #%>% 
      #addCircles(data = smog, lat = ~ latitude, lng = ~ longitude, weight = 1, radius = ~sqrt(as.numeric(wskaznik))*5000, popup = ~as.character(smog$wskaznik), label = ~as.character(paste0(city, ': ', "PM10: ", sep = " ", as.numeric(smog$wskaznik))), color = 'dark orange', fillOpacity = 0.5)
  })
  
   observe({
    smog<-data1()
    smog <- smog %>% dplyr::filter(input$variable == as.character(smog$data))
    smog$odczyt<-ifelse(smog$odczyt<0,0,smog$odczyt)
    proxy <- leafletProxy("mymap", data = smog)
    proxy %>% clearMarkers()
    #if (input$markers) {
      proxy %>% addCircleMarkers(stroke = FALSE, color = ~pal2(smog$wskaznik), fillOpacity = 0.8,      label = ~as.character(paste0(toupper(smog$city),": ",smog$wskaznik,'/PM2.5',"  - ", sep = " ", round(odczyt,2), "/", ifelse(odczyt2 %in% c('-'),'-',round(as.numeric(odczyt2),2))))) #%>%

  })
    observe({
      dat<-data1()
      
    updateSelectInput(session, "variable",
                      choices = as.character(sort(as.POSIXlt(as.POSIXct(as.character(unique(dat[,2])),format="%Y-%m-%d %H:%M:%S"))))
    )
  })
  

  observeEvent(input$variable, {
    print(paste0("You have chosen: ", input$variable))
  })  
}

shinyApp(ui = ui, server = server)

