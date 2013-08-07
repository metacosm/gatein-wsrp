/*
* JBoss, a division of Red Hat
* Copyright 2012, Red Hat Middleware, LLC, and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/

package org.gatein.wsrp.consumer;

import org.gatein.common.util.Version;
import org.gatein.pc.api.InvokerUnavailableException;
import org.gatein.wsrp.consumer.handlers.ProducerSessionInformation;
import org.gatein.wsrp.handler.RequestHeaderClientHandler;
import org.gatein.wsrp.services.MarkupService;
import org.gatein.wsrp.services.PortletManagementService;
import org.gatein.wsrp.services.RegistrationService;
import org.gatein.wsrp.services.SOAPServiceFactory;
import org.gatein.wsrp.services.ServiceDescriptionService;
import org.gatein.wsrp.services.ServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:chris.laprun@jboss.com">Chris Laprun</a>
 */
public class EndpointConfigurationInfo
{
   private static final Logger log = LoggerFactory.getLogger(EndpointConfigurationInfo.class);

   // transient variables
   /** The ordered (in the given order of URLs) map of ServiceFactories */
   private transient LinkedHashMap<String, ServiceFactory> urlToServiceFactory = new LinkedHashMap<String, ServiceFactory>();

   /**
    * Used to implement a simple round-robin-like mechanism to switch producer URL in case one is down
    */
   private transient List<String> allWSDLURLs = Collections.emptyList();
   private transient int currentURL;
   private static final String SEPARATOR = " ";
   private transient String wsdlURL;
   private int msBeforeTimeOut;

   private final ServiceFactory factoryPrototype;

   public EndpointConfigurationInfo(ServiceFactory serviceFactory)
   {
      factoryPrototype = serviceFactory;
      wsdlURL = serviceFactory.getWsdlDefinitionURL();
      msBeforeTimeOut = serviceFactory.getWSOperationTimeOut();
   }

   public EndpointConfigurationInfo()
   {
      this(new SOAPServiceFactory());
   }

   public String getWsdlDefinitionURL()
   {
      return wsdlURL;
   }

   public String getEffectiveWSDLURL()
   {
      return getServiceFactory().getWsdlDefinitionURL();
   }

   /**
    * When the WSDL URL changes, we need
    *
    * @param wsdlDefinitionURL
    */
   public void setWsdlDefinitionURL(String wsdlDefinitionURL)
   {
      if (wsdlDefinitionURL != null)
      {
         if (wsdlDefinitionURL.contains(SEPARATOR))
         {
            // we have a URL with a separator to support passing several URLs for a simple failover mechanism, so we need to extract the individual URLs
            final String[] urls = wsdlDefinitionURL.split("\\s+");
            // make sure we don't have duplicates
            allWSDLURLs = new ArrayList<String>(urls.length);
            for (String url : urls)
            {
               if (!allWSDLURLs.contains(url))
               {
                  allWSDLURLs.add(url);
               }
            }

            // copy the existing ServiceFactories
            Map<String, ServiceFactory> existing = new HashMap<String, ServiceFactory>(urlToServiceFactory);
            // clear existing
            urlToServiceFactory.clear();
            // add ServiceFactories for each URL, keeping the ones we already have when a URL already existed
            // we re-add things to keep potentially new URL order
            for (String url : allWSDLURLs)
            {
               // retrieve the existing ServiceFactory
               ServiceFactory serviceFactory = existing.get(url);
               if (serviceFactory == null)
               {
                  // if we don't already have a ServiceFactory for this URL, create one
                  serviceFactory = createFromPrototype();
                  serviceFactory.setWsdlDefinitionURL(url);
                  serviceFactory.setWSOperationTimeOut(msBeforeTimeOut);
               }
               urlToServiceFactory.put(url, serviceFactory);

            }
         }
         else
         {
            // we only have one URL
            allWSDLURLs = Collections.singletonList(wsdlDefinitionURL);

            // remove all ServiceFactories
            urlToServiceFactory.clear();

            // add a new one with the proper WSDL URL
            ServiceFactory serviceFactory = createFromPrototype();
            serviceFactory.setWsdlDefinitionURL(wsdlDefinitionURL);
            serviceFactory.setWSOperationTimeOut(msBeforeTimeOut);
            urlToServiceFactory.put(wsdlDefinitionURL, serviceFactory);
         }
      }

      // reset current URL pointer
      currentURL = 0;
      wsdlURL = wsdlDefinitionURL;
   }

   private ServiceFactory createFromPrototype()
   {
      return factoryPrototype.clone();
   }

   public List<String> getAllWSDLURLs()
   {
      return allWSDLURLs;
   }

   public void start() throws Exception
   {

      List<ServiceFactory> factories = new ArrayList<ServiceFactory>(urlToServiceFactory.values());
      for (ServiceFactory factory : factories)
      {
         try
         {
            factory.start();
         }
         catch (Exception e)
         {
            removeServiceFactory(factory);
         }
      }

      // all factories should use the same WSRP version and WSS availabilty
      final ServiceFactory factory = factories.get(0);
      initStateFrom(factory);
   }

   private void initStateFrom(ServiceFactory factory)
   {
      msBeforeTimeOut = factory.getWSOperationTimeOut();
   }

   private void removeServiceFactory(ServiceFactory factory)
   {
      final String url = factory.getWsdlDefinitionURL();
      removeServiceFactoryFor(url);
   }

   private void removeServiceFactoryFor(String url)
   {
      log.info("ServiceFactory for URL '" + url + "' is not available. Removing it from round-robin.");
      urlToServiceFactory.remove(url); // todo: implement re-adding after a cooldown period
      final int index = allWSDLURLs.indexOf(url);
      allWSDLURLs.remove(url);

      // if, after removing the URL, we don't have any anymore, throw an exception since we cannot do anything anymore
      if (allWSDLURLs.isEmpty())
      {
         throw new RuntimeException("Couldn't find an available ServiceFactory!"); // todo: improve error message / deal with this condition better
      }

      // compute next URL to use
      final int urlNumber = getNumberOfWSDLURLs();
      currentURL = index + 1;
      StringBuilder sb = new StringBuilder(urlNumber * 128);
      int i = 0;
      for (String wsdl : allWSDLURLs)
      {
         sb.append(wsdl);
         if (i++ != urlNumber - 1)
         {
            sb.append(SEPARATOR);
         }
      }
      wsdlURL = sb.toString();
   }

   public void stop() throws Exception
   {
      for (ServiceFactory factory : urlToServiceFactory.values())
      {
         factory.stop();
      }
   }

   ServiceFactory getServiceFactory()
   {
      return getServiceFactory(true);
   }

   ServiceFactory getServiceFactory(boolean start)
   {
      // figure out which ServiceFactory to use

      // first, check if there's already a ServiceFactory associated with the current session information to have sticky behavior
      final ProducerSessionInformation sessionInfo = RequestHeaderClientHandler.getProducerSessionInformation(true);
      ServiceFactory factory = sessionInfo.getServiceFactory();

      // only use the factory from the session information if we have configured URLs and the factory is still part of the set of available factories
      if (factory != null && !allWSDLURLs.isEmpty() && !urlToServiceFactory.containsKey(factory.getWsdlDefinitionURL()))
      {
         // remove the factory from the session information
         sessionInfo.setServiceFactory(null);
         // set the factory to null so that another one can be picked
         factory = null;
      }

      // TODO: (not here, though) deal with expired sessions => need a listener for session events
      // TODO: need to be able to detect that a ServiceFactory previously failed when we enter this method to update status
      // TODO: multithreading proof!

      // if we haven't found a ServiceFactory, pick one from the available ones
      if (factory == null)
      {
         final int size = getNumberOfWSDLURLs();
         if (size == 0)
         {
            // if we don't have a list of URLs to work with yet, return a new instance based on prototype
            factory = createFromPrototype();
         }
         else
         {
            // get the ServiceFactory associated with the currently selected URL
            factory = urlToServiceFactory.get(allWSDLURLs.get(currentURL % size));
            // increment pointer to current URL
            currentURL++;
         }
      }

      // make sure the factory is started if we intend to use it
      if (start && !factory.isAvailable())
      {
         try
         {
            factory.start();
            initStateFrom(factory);
         }
         catch (Exception e)
         {
            // factory didn't start properly, remove it from available ones and attempt to retrieve another one by recursively calling ourselves
            removeServiceFactory(factory);
            return getServiceFactory(true);
         }
      }

      // if everything went well, associate the factory we picked with the current session information
      sessionInfo.setServiceFactory(factory);

      return factory;
   }

   ServiceDescriptionService getServiceDescriptionService() throws InvokerUnavailableException
   {
      try
      {
         return getServiceFactory().getServiceDescriptionService();
      }
      catch (Exception e)
      {
         throw new InvokerUnavailableException("Couldn't access ServiceDescription service. Cause: "
            + e.getLocalizedMessage(), e);
      }
   }

   MarkupService getMarkupService() throws InvokerUnavailableException
   {
      try
      {
         return getServiceFactory().getMarkupService();
      }
      catch (Exception e)
      {
         throw new InvokerUnavailableException("Couldn't access Markup service. Cause: "
            + e.getLocalizedMessage(), e);
      }
   }

   PortletManagementService getPortletManagementService() throws InvokerUnavailableException
   {
      try
      {
         return getServiceFactory().getPortletManagementService();
      }
      catch (Exception e)
      {
         throw new InvokerUnavailableException("Couldn't access PortletManagement service. Cause: "
            + e.getLocalizedMessage(), e);
      }
   }

   RegistrationService getRegistrationService() throws InvokerUnavailableException
   {
      try
      {
         return getServiceFactory().getRegistrationService();
      }
      catch (Exception e)
      {
         throw new InvokerUnavailableException("Couldn't access Registration service. Cause: "
            + e.getLocalizedMessage(), e);
      }
   }

   public boolean isAvailable()
   {
      return getServiceFactory(false).isAvailable();
   }

   public boolean isRefreshNeeded()
   {
      // we need a refresh if we're not available and the service factory is not in a failed state
      boolean result = !isAvailable() /*&& !getServiceFactory(false).isFailed()*/;
      if (result && log.isDebugEnabled())
      {
         log.debug("Refresh needed");
      }
      return result;
   }

   public boolean refresh() throws InvokerUnavailableException
   {
      return isRefreshNeeded() && forceRefresh();
   }

   boolean forceRefresh() throws InvokerUnavailableException
   {
      try
      {
         return getServiceFactory().refresh(true);
      }
      catch (Exception e)
      {
         throw new InvokerUnavailableException(e);
      }
   }

   public String getRemoteHostAddress()
   {
      // extract host URL
      String wsdl = getWsdlDefinitionURL();
      int hostBegin = wsdl.indexOf("://") + 3;
      return wsdl.substring(0, wsdl.indexOf('/', hostBegin));
   }

   /**
    * Number of milliseconds before a WS operation is considered as having timed out.
    *
    * @param msBeforeTimeOut number of milliseconds to wait for a WS operation to return before timing out. Will be set
    *                        to {@link ServiceFactory#DEFAULT_TIMEOUT_MS} if negative.
    */
   public void setWSOperationTimeOut(int msBeforeTimeOut)
   {
      this.msBeforeTimeOut = msBeforeTimeOut;
      for (ServiceFactory factory : urlToServiceFactory.values())
      {
         factory.setWSOperationTimeOut(msBeforeTimeOut);
      }
   }

   public int getWSOperationTimeOut()
   {
      return msBeforeTimeOut;
   }

   Version getWSRPVersion()
   {
      return getServiceFactory().getWSRPVersion();
   }

   public boolean isLoadbalancing()
   {
      return allWSDLURLs.size() > 1;
   }

   public int getNumberOfWSDLURLs()
   {
      return allWSDLURLs.size();
   }

   public boolean switchProducerIfPossible()
   {
      // assume that we have a failed ServiceFactory so remove it from the current session and from the set of available factories
      final ProducerSessionInformation sessionInformation = RequestHeaderClientHandler.getCurrentProducerSessionInformation();
      removeServiceFactory(sessionInformation.getServiceFactory());
      sessionInformation.setServiceFactory(null);

      // try to get a ServiceFactory, if we get one, we successfully switched producer if we assumed the current ServiceFactory had failed...
      return getServiceFactory() != null;
   }
}
