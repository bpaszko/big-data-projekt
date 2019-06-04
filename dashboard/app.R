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

# Define UI for application that draws a histogram
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
    #this allows me to put the checkmarks ontop of the map to allow people to view earthquake depth or overlay a heatmap
    #absolutePanel(top = 60, left = 20, 
                 # checkboxInput("markers", "Depth", FALSE),
                 # checkboxInput("heat", "Heatmap", FALSE)
    #),
    width = 8
  ))
)

server <- function(input, output, session) {

  data1 =reactive({
    dt<-read.csv(url("https://storage.googleapis.com/smog_pred_bucket/smog_pred.csv"), encoding="UTF-8",sep = ',')
    #choices = sort(as.POSIXlt(as.POSIXct(as.character(unique(dt[,1])),format="%Y-%m-%d %H:%M:%S")))
    colnames(dt)=c('data','city','odczyt','wskaznik','datapob')
    dt<-merge(dt,dat1)
    #dt<-merge(dt, data1)
    return (dt)
  })  
  #define the color pallate for the magnitidue of the earthquake
  pal <- colorNumeric(
    palette = c('gold', 'orange', 'dark orange', 'orange red', 'red', 'dark red'),
    domain = data$mag)
  
  #define the color of for the depth of the earquakes
  pal2 <- colorFactor(
    palette = c('blue', 'yellow', 'red'),
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
  
  #next we use the observe function to make the checkboxes dynamic. If you leave this part out you will see that the checkboxes, when clicked on the first time, display our filters...But if you then uncheck them they stay on. So we need to tell the server to update the map when the checkboxes are unchecked.
  observe({
    smog<-data1()
    smog <- smog %>% dplyr::filter(input$variable == as.character(smog$data))
    smog$odczyt<-ifelse(smog$odczyt<0,0,smog$odczyt)
    proxy <- leafletProxy("mymap", data = smog)
    proxy %>% clearMarkers()
    #if (input$markers) {
      proxy %>% addCircleMarkers(stroke = FALSE, color = ~pal2(smog$wskaznik), fillOpacity = 0.9,      label = ~as.character(paste0(toupper(smog$city),": ",smog$wskaznik,'/PM2.5',"  - ", sep = " ", round(odczyt,2), "/", round(odczyt,2)))) #%>%
        #addLegend("bottomright", pal = pal2, values = smog$wskaznik,
                  #title = "Depth Type",
                 # opacity = 1)#}
    #else {
      #proxy %>% clearMarkers() %>% clearControls()
    #}
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

# Run the application 
shinyApp(ui = ui, server = server)

